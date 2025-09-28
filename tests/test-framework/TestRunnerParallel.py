# Databricks notebook source
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from TableAndViewCreator import TableAndViewCreator
from MetadataLoader import load_test_metadata, get_scripts_config

spark.conf.set("spark.sql.session.timeZone", "America/Phoenix")
target_catalog = dbutils.widgets.get("unittest_catalog")

# COMMAND ----------

# Get test metadata and volume path configuration
test_metadata = load_test_metadata()
test_data_volume = test_metadata.get("test_data_volume", f"/Volumes/{target_catalog}/default/test_data").format(target_catalog=target_catalog)

def run_single_test(src_dir: str, folder: str, task_file_name: str, test_name: str, test_catalog: str, test_data_volume: str):
    """
    Run a single test with its own target catalog parameter.
    This function will be executed in parallel for each test.
    """

    
    try:
        # run DataPrep.py (which now includes catalog and schema preparation)
        print(f"Running DataPrep for {task_file_name} and {test_name} with catalog {test_catalog}")
        dbutils.notebook.run("../../tests/test-framework/DataPrep", 600, {
            "catalog": test_catalog, 
            "task_file_name": task_file_name, 
            "test_name": test_name, 
            "volume_path": test_data_volume, 
            "definitions_volume": test_data_volume
        })
        
        print(f"Running ETL Job for {task_file_name} and {test_name} with catalog {test_catalog}")
        # run ETL job
        dbutils.notebook.run(f"../../{src_dir}/{folder}/{task_file_name}", 600, {"catalog": test_catalog})
        
        # run TestValidation.py
        print(f"Running TestValidation for {task_file_name} and {test_name} with catalog {test_catalog}")
        dbutils.notebook.run("../../tests/test-framework/TestValidation", 600, {
            "catalog": test_catalog, 
            "task_file_name": task_file_name, 
            "test_name": test_name,
            "volume_path": test_data_volume
        })
        
        return {"success": True, "test": f"{task_file_name}_{test_name}", "catalog": test_catalog}
        
    except Exception as e:
        error_msg = f"❌ Failed test {task_file_name} {test_name} with catalog {test_catalog}: {str(e)}"
        print(error_msg)
        return {"success": False, "test": f"{task_file_name}_{test_name}", "catalog": test_catalog, "error": str(e)}

# COMMAND ----------

# Configuration for parallel execution
MAX_WORKERS = int(dbutils.widgets.get("max_workers") if dbutils.widgets.get("max_workers") else "4")
print(f"Running tests in parallel with max {MAX_WORKERS} workers")

# Get enabled tests from test metadata
enabled_tests = test_metadata.get("enabled_tests", [])
src_dir = test_metadata.get("src_dir", "src")

# Get scripts configuration from script metadata
scripts_config = get_scripts_config()

# Prepare test tasks for parallel execution
test_tasks = []
for enabled_test in enabled_tests:
    script_name = enabled_test["script"]
    folder = enabled_test["folder"]
    for test_name in enabled_test["tests"]:
        # Create a unique catalog for each test to avoid conflicts
        test_catalog = f"{target_catalog}_{script_name}_{test_name}".replace(".", "_").replace("-", "_")
        test_tasks.append({
            "folder": folder,
            "script_name": script_name,
            "test_name": test_name,
            "test_catalog": test_catalog,
            "test_data_volume": test_data_volume
        })

print(f"Prepared {len(test_tasks)} test tasks for parallel execution")

# COMMAND ----------

# Execute tests in parallel
results = []
failed_tests = []

with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    # Submit all tasks
    future_to_task = {
        executor.submit(
            run_single_test, 
            src_dir,
            task["folder"], 
            task["script_name"], 
            task["test_name"], 
            task["test_catalog"],
            task["test_data_volume"]
        ): task for task in test_tasks
    }
    
    # Collect results as they complete
    for future in as_completed(future_to_task):
        task = future_to_task[future]
        try:
            result = future.result()
            results.append(result)
            
            if result["success"]:
                print(f"✅ Successfully completed {result['test']} with catalog {result['catalog']}")
            else:
                failed_tests.append(result)
                print(f"❌ Failed {result['test']} with catalog {result['catalog']}: {result['error']}")
                
        except Exception as e:
            error_result = {
                "success": False, 
                "test": f"{task['script_name']}_{task['test_name']}", 
                "catalog": task["test_catalog"], 
                "error": str(e)
            }
            failed_tests.append(error_result)
            print(f"❌ Exception in {task['script_name']}_{task['test_name']}: {str(e)}")

# COMMAND ----------

# Summary and final results
total_tests = len(test_tasks)
successful_tests = len([r for r in results if r["success"]])
failed_count = len(failed_tests)

print(f"\n{'='*50}")
print(f"PARALLEL TEST EXECUTION SUMMARY")
print(f"{'='*50}")
print(f"Total tests: {total_tests}")
print(f"Successful: {successful_tests}")
print(f"Failed: {failed_count}")
print(f"Success rate: {(successful_tests/total_tests)*100:.1f}%")

if failed_tests:
    print(f"\n❌ FAILED TESTS:")
    for failed_test in failed_tests:
        print(f"  - {failed_test['test']} (catalog: {failed_test['catalog']}): {failed_test['error']}")
    
    raise Exception(f"❌ {failed_count} out of {total_tests} tests failed")
else:
    print(f"\n✅ All {total_tests} tests passed successfully!")