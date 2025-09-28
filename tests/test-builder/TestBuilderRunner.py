# Databricks notebook source
"""
Test Builder Runner

This script runs the test builder to automatically generate test data.
It can be executed with different parameters via widgets.

Usage:
- The script will read config.json and script_metadata.json from the current directory
"""

# COMMAND ----------

import json
# Set timezone
spark.conf.set("spark.sql.session.timeZone", "America/Phoenix")

# COMMAND ----------

# Load configuration
config_path = "config.json"
with open(config_path, 'r') as f:
    config = json.load(f)

target_catalog = config.get("target_catalog")
unit_test_catalog = config.get("unit_test_catalog")
output_volume = config.get("output_volume")
definitions_volume = config.get("definitions_volume")
test_name = config.get("test_name", "test_basic")
print(f"Target Catalog: {target_catalog}")
print(f"Unit Test Catalog: {unit_test_catalog}")
print(f"Output Volume: {output_volume}")
print(f"Definitions Volume: {definitions_volume}")

# COMMAND ----------

# Build test data for all scripts
print("🚀 Starting test data generation...")

# Get enabled tests from config
tests = config.get("tests", [])
enabled_tests = [t for t in tests if t.get("enabled", False)]

print(f"📊 Processing {len(enabled_tests)} enabled tests")

failed_tests = []
src_dir = config.get("src_dir")

# Loop through each enabled test
for i, test_config in enumerate(enabled_tests, 1):
    script_name = test_config.get("script")
    folder = test_config.get("folder")
    test_name = test_config.get("test_name")
    
    if not script_name or not folder or not test_name:
        print(f"❌ Invalid test configuration: {test_config}")
        continue
    
    print(f"\n🔨 Processing test {i}/{len(enabled_tests)}: {script_name}")
    print(f"  📁 Folder: {folder}")
    print(f"  🧪 Test Name: {test_name}")
    
    try:
        # Step 1: Dump Pre-Test Data
        print(f"  📋 Step 1: Dumping pre-test data for {script_name}:{test_name}")
        dbutils.notebook.run("DumpPreTestData", 3600, {
            "script_name": script_name,
            "test_name": test_name
        })
        print(f"  ✅ Pre-test data dumped successfully")
        
        # Step 2: Prepare Unit Test Environment
        print(f"  🏗️  Step 2: Preparing unit test environment for {script_name}:{test_name}")
        dbutils.notebook.run("../../tests/test-framework/DataPrep", 1200, {
            "catalog": unit_test_catalog, 
            "task_file_name": script_name, 
            "test_name": test_name,
            "volume_path": output_volume,
            "definitions_volume": definitions_volume
        })
        print(f"  ✅ Unit test environment prepared successfully")
        
        # Step 3: Run Unit Test
        print(f"  🧪 Step 3: Running unit test for {script_name}:{test_name}")
        dbutils.notebook.run(f"../../{src_dir}/{folder}/{script_name}", 1200, {
            "catalog": unit_test_catalog
        })
        print(f"  ✅ Unit test executed successfully")
        
        # Step 4: Dump Post-Test Data
        print(f"  📋 Step 4: Dumping post-test data for {script_name}:{test_name}")
        dbutils.notebook.run("DumpPostTestData", 1200, {
            "target_catalog": unit_test_catalog,
            "output_volume": output_volume,
            "script_name": script_name,
            "folder": folder,
            "test_name": test_name
        })
        print(f"  ✅ Post-test data dumped successfully")
        
        print(f"✅ Successfully build test data for: {script_name}:{test_name}")
        
    except Exception as e:
        print(f"❌ Failed to build test for {script_name}: {e}")
        failed_tests.append(script_name)

# Report results
if failed_tests:
    print(f"\n❌ Failed to process {len(failed_tests)} test(s): {', '.join(failed_tests)}")
    raise Exception(f"Failed to process tests: {', '.join(failed_tests)}")
else:
    print(f"\n✅ Successfully processed all {len(enabled_tests)} test(s)")

print("✅ Test data generation completed successfully!")