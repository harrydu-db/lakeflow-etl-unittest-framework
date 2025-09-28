# Databricks notebook source
import os
from TestBuilder import TestBuilder
from TestBuilderAddon import TestBuilderAddon

# COMMAND ----------

# Initialize TestBuilder
script_name = dbutils.widgets.get("script_name")
test_name = dbutils.widgets.get("test_name")
config_path = "config.json"
script_metadata_path = "script_metadata.json"

test_builder = TestBuilder(
    config_path=config_path,
    script_metadata_path=script_metadata_path
)

# COMMAND ----------
# Dump pre-test data for reference tables
print(f"🔄 Dumping pre-test data for script: {script_name} and test: {test_name}")



# COMMAND ----------
# Set timezone
spark.conf.set("spark.sql.session.timeZone", "America/Phoenix")
spark.sql(f"use catalog {test_builder.target_catalog}")

# Step 1: Resolve filters
resolved_filters = test_builder.build_filters(script_name, test_name)

# Step 2: Dump tables that are referenced in the script
test_builder.dump_pretest_tables(script_name, resolved_filters, test_name)
    
# Step 3: Run addon (including TASK_CONTROL table creation)
print(f"\n  🔄 Running addon")
TestBuilderAddon.run_addon(test_builder, script_name, test_name)

print(f"✅ Pre-test data dumping completed for script: {script_name} and test: {test_name}")
