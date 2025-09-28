# Databricks notebook source
import os
from TableAndViewCreator import TableAndViewCreator
from MetadataLoader import get_scripts_config, load_test_metadata

# COMMAND ----------
target_catalog = dbutils.widgets.get("catalog")
task_file_name = dbutils.widgets.get("task_file_name")
test_name = dbutils.widgets.get("test_name")
volume_path = dbutils.widgets.get("volume_path")
# Optional definitions_volume parameter - defaults to volume_path if not provided
definitions_volume = dbutils.widgets.get("definitions_volume") or volume_path
spark.conf.set("spark.sql.session.timeZone", "America/Phoenix")

# COMMAND ----------
# Load scripts configuration from metadata
scripts_config = get_scripts_config()

# Load test metadata to get schemas configuration
test_metadata = load_test_metadata()
schemas = test_metadata.get("schemas", [])

creator = TableAndViewCreator()

# Prepare catalog and schemas first
print(f"Preparing catalog and schemas for {target_catalog}")
if not creator.prepare_catalog_and_schemas(spark, target_catalog, schemas):
    raise Exception(f"Failed to prepare catalog and schemas for {target_catalog}")

# Get the script configuration for the current task_file_name
if task_file_name not in scripts_config:
    raise ValueError(f"Task file name '{task_file_name}' not found in scripts configuration")

script_config = scripts_config[task_file_name]


# COMMAND ----------

# Extract common additional tables from test metadata
common_tables = []
if test_metadata and "common_additional_script_metadata" in test_metadata:
    common_tables = test_metadata["common_additional_script_metadata"].get("tables", [])

# Process tables and views
rtn = creator.create_tables_and_views(spark, scripts_config, task_file_name, test_name, target_catalog, volume_path, definitions_volume, common_tables)

if rtn == False:
    raise Exception("Failed to create tables and views")






