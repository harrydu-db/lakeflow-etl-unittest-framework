# Databricks notebook source
import os
from TableAndViewCreator import TableAndViewCreator
from MetadataLoader import load_test_metadata, get_scripts_config

spark.conf.set("spark.sql.session.timeZone", "America/Phoenix")
target_catalog = dbutils.widgets.get("unittest_catalog")

# COMMAND ----------

# Get enabled tests from test metadata
test_metadata = load_test_metadata()
test_data_volume = test_metadata.get("test_data_volume", f"/Volumes/{target_catalog}/default/test_data").format(target_catalog=target_catalog)

def run_single_test(src_dir: str, folder: str, task_file_name: str, test_name: str, volume_path: str):
    # run DataPrep.py
    print(f"Running DataPrep for {task_file_name} and {test_name}")
    dbutils.notebook.run("../../tests/test-framework/DataPrep", 600, {"catalog": target_catalog, "task_file_name": task_file_name, "test_name": test_name, "volume_path": volume_path, "definitions_volume": volume_path})
    
    print(f"Running ETL Job for {task_file_name} and {test_name}")
    # run ETL job
    dbutils.notebook.run(f"../../{src_dir}/{folder}/{task_file_name}", 600, {"catalog": target_catalog})
    
    # run TestValidation.py
    print(f"Running TestValidation for {task_file_name} and {test_name}")
    dbutils.notebook.run("../../tests/test-framework/TestValidation", 600, {"catalog": target_catalog, "task_file_name": task_file_name, "test_name": test_name, "volume_path": volume_path})

# COMMAND ----------
failed = False
passed_tests = 0
failed_tests = 0

# Get enabled tests from test metadata
enabled_tests = test_metadata.get("enabled_tests", [])

# Get scripts configuration from script metadata
scripts_config = get_scripts_config()

# Count total tests
total_tests = 0
for enabled_test in enabled_tests:
    total_tests += len(enabled_test["tests"])

print(f"🚀 Starting test execution...")
print(f"📊 Total tests to run: {total_tests}")
print("=" * 60)

current_test = 0

src_dir = test_metadata.get("src_dir", "src")
# Run only the enabled tests
for enabled_test in enabled_tests:
    script_name = enabled_test["script"]
    folder = enabled_test["folder"]
    for test_name in enabled_test["tests"]:
        current_test += 1
        print(f"\n🔍 [{current_test}/{total_tests}] Starting test: {script_name} - {test_name}")
        print(f"📁 Folder: {folder}")
        print("-" * 40)
        
        try:
            run_single_test(src_dir, folder, script_name, test_name, test_data_volume)
            passed_tests += 1
            print(f"✅ [{current_test}/{total_tests}] Test PASSED: {script_name} - {test_name}")
        except Exception as e:
            failed = True
            failed_tests += 1
            print(f"❌ [{current_test}/{total_tests}] Test FAILED: {script_name} - {test_name}")
            print(f"   Error: {str(e)}")
            continue
        
# Print final summary
print("\n" + "=" * 60)
print("📋 TEST EXECUTION SUMMARY")
print("=" * 60)
print(f"✅ Passed: {passed_tests}")
print(f"❌ Failed: {failed_tests}")
print(f"📊 Total:  {total_tests}")
print(f"📈 Success Rate: {(passed_tests/total_tests)*100:.1f}%" if total_tests > 0 else "📈 Success Rate: N/A")

if failed:
    print("\n❌ Some tests failed!")
    raise Exception("❌ Failed tests")
else:
    print("\n🎉 All tests passed successfully!")