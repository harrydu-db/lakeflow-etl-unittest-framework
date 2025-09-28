# Databricks notebook source
import os
from TestBuilder import TestBuilder

# COMMAND ----------

# Get parameters from widgets
target_catalog = dbutils.widgets.get("target_catalog")
output_volume = dbutils.widgets.get("output_volume")
script_name = dbutils.widgets.get("script_name")
folder = dbutils.widgets.get("folder")
test_name = dbutils.widgets.get("test_name")

# Validate required parameters
if not target_catalog:
    raise ValueError("target_catalog widget is required")
if not output_volume:
    raise ValueError("output_volume widget is required")
if not script_name:
    raise ValueError("script_name widget is required")
if not folder:
    raise ValueError("folder widget is required")

print(f"🎯 Target Catalog: {target_catalog}")
print(f"📁 Output Volume: {output_volume}")
print(f"📋 Script Name: {script_name}")
print(f"📁 Folder: {folder}")

# COMMAND ----------

# Set timezone
spark.conf.set("spark.sql.session.timeZone", "America/Phoenix")
spark.sql(f"use catalog {target_catalog}")

# COMMAND ----------

# Initialize TestBuilder
config_path = "config.json"
script_metadata_path = "script_metadata.json"

test_builder = TestBuilder(
    config_path=config_path,
    script_metadata_path=script_metadata_path
)

# COMMAND ----------

# Dump post-test data for update tables without filters
print(f"🔄 Dumping post-test data for script: {script_name}")

if script_name not in test_builder.script_metadata:
    print(f"❌ Script {script_name} not found in metadata")
    raise ValueError(f"Script {script_name} not found in metadata")

# Get test name from widget

# Dump all update_tables as-is without any filters
success = test_builder.dump_posttest_tables(script_name, test_name)

print(f"✅ Post-test data dumping completed for script: {script_name}")
