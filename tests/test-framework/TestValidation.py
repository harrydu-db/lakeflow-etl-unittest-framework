# Databricks notebook source
from DataValidator import DataValidator


# COMMAND ----------

# Example usage:
# Get parameters from widgets
target_catalog = dbutils.widgets.get("catalog")
volume_path = dbutils.widgets.get("volume_path")
task_file_name = dbutils.widgets.get("task_file_name")
test_name = dbutils.widgets.get("test_name")
spark.conf.set("spark.sql.session.timeZone", "America/Phoenix")

# Initialize validation
validator = DataValidator(spark, target_catalog, volume_path)

# Run validation for all tables
validation_results = validator.validate_all_tables(task_file_name, test_name)

# Save validation report
validator.save_validation_report(task_file_name, test_name)

# Check validation results and exit with appropriate status
failed_tables = []
for table_name, result in validation_results.items():
    if not result.get("validation_passed", False):
        failed_tables.append(table_name)

if failed_tables:
    error_message = f"Validation failed for {len(failed_tables)} table(s): {', '.join(failed_tables)}"
    print(f"❌ {error_message}")
    # dbutils.notebook.exit(error_message)
    raise Exception(error_message)
else:
    success_message = f"Validation completed successfully for all {len(validation_results)} table(s)"
    print(f"✅ {success_message}")
    # dbutils.notebook.exit(success_message)
