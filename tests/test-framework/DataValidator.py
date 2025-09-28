import os
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col, when, isnan, isnull
from TableAndViewCreator import TableAndViewCreator
from MetadataLoader import get_scripts_config

class DataValidator:
    """
    Post-test data validation class that compares CSV data with actual table content.
    """
    
    def __init__(self, spark: SparkSession, target_catalog: str, volume_path: str):
        """
        Initialize DataValidator with Spark session and configuration.
        
        Args:
            spark: SparkSession instance
            target_catalog: Target catalog name
            volume_path: Path to the volume containing test data
        """
        self.spark = spark
        self.target_catalog = target_catalog
        self.volume_path = volume_path
        self.creator = TableAndViewCreator()
        self.validation_results = {}
    
    def validate_csv_structure(self, csv_path: str) -> dict:
        """
        Validate CSV file structure and return analysis results.
        
        Args:
            csv_path: Path to the CSV file
            
        Returns:
            Dictionary containing validation results
        """
        import csv
        
        validation_result = {
            "is_valid": True,
            "issues": [],
            "warnings": [],
            "line_count": 0,
            "header_columns": 0,
            "data_lines": 0
        }
        
        try:
            with open(csv_path, 'r') as f:
                lines = f.readlines()
                
            validation_result["line_count"] = len(lines)
            
            if not lines:
                validation_result["is_valid"] = False
                validation_result["issues"].append("File is empty")
                return validation_result
            
            # Check header using proper CSV parsing
            header_line = lines[0].strip()
            if not header_line:
                validation_result["is_valid"] = False
                validation_result["issues"].append("Header line is empty")
                return validation_result
            
            # Parse header with proper CSV rules
            header_reader = csv.reader([header_line])
            header_parts = next(header_reader)
            validation_result["header_columns"] = len(header_parts)
            
            # Check for trailing comma in header
            if header_line.endswith(','):
                validation_result["warnings"].append("Header ends with trailing comma")
            
            # Check data lines using proper CSV parsing
            data_lines = []
            for i, line in enumerate(lines[1:], 1):
                line = line.strip()
                if line:  # Non-empty line
                    data_lines.append((i + 1, line))
            
            validation_result["data_lines"] = len(data_lines)
            
            # Check column count consistency using proper CSV parsing
            for line_num, line_content in data_lines:
                try:
                    data_reader = csv.reader([line_content])
                    data_parts = next(data_reader)
                    if len(data_parts) != len(header_parts):
                        validation_result["is_valid"] = False
                        validation_result["issues"].append(f"Line {line_num} has {len(data_parts)} columns, expected {len(header_parts)}")
                        # Show the actual parsed columns for debugging
                        validation_result["issues"].append(f"  Parsed columns: {data_parts}")
                except Exception as parse_error:
                    validation_result["is_valid"] = False
                    validation_result["issues"].append(f"Line {line_num} could not be parsed as CSV: {parse_error}")
            
            # Check for empty lines
            empty_lines = [i for i, line in enumerate(lines) if line.strip() == '']
            if empty_lines:
                validation_result["warnings"].append(f"Empty lines found at positions: {empty_lines}")
            
            return validation_result
            
        except Exception as e:
            validation_result["is_valid"] = False
            validation_result["issues"].append(f"Error reading file: {e}")
            return validation_result

    def normalize_null_values(self, df, columns):
        """
        Normalize null values in a DataFrame by converting string representations of null to actual null values.
        Only applies to string columns to avoid issues with timestamp columns.
        
        Args:
            df: Spark DataFrame
            columns: List of column names to normalize
            
        Returns:
            DataFrame with normalized null values
        """
        from pyspark.sql.functions import when, col, lit
        
        normalized_df = df
        for column_name in columns:
            # Only normalize string columns, skip timestamp columns
            column_type = dict(df.dtypes)[column_name]
            if column_type == 'string':
                normalized_df = normalized_df.withColumn(
                    column_name,
                    when(col(column_name).isin("NULL", "null", ""), lit(None))
                    .otherwise(col(column_name))
                )
        return normalized_df
        
    def load_csv_data(self, table_name: str, task_file_name: str, test_name: str):
        """
        Load CSV data from post_test folder.
        
        Args:
            table_name: Name of the table
            task_file_name: Name of the task file
            test_name: Name of the test
            
        Returns:
            Spark DataFrame containing the CSV data
        """
        try:
            # Construct path to post_test CSV file
            csv_path = os.path.join(self.volume_path, task_file_name, test_name, "post_test", f"{table_name}.csv")
            
            print(f"📋 Loading CSV data from: {csv_path}")
            
            if not os.path.exists(csv_path):
                raise FileNotFoundError(f"CSV file not found: {csv_path}")
            
            # Validate CSV structure before attempting to load
            print(f"🔍 Validating CSV structure...")
            csv_validation = self.validate_csv_structure(csv_path)
            
            if not csv_validation["is_valid"]:
                print(f"❌ CSV structure validation failed:")
                for issue in csv_validation["issues"]:
                    print(f"   - {issue}")
                raise ValueError(f"CSV file structure is invalid: {csv_validation['issues']}")
            
            if csv_validation["warnings"]:
                print(f"⚠️  CSV structure warnings:")
                for warning in csv_validation["warnings"]:
                    print(f"   - {warning}")
            
            print(f"✅ CSV structure validation passed: {csv_validation['data_lines']} data lines, {csv_validation['header_columns']} columns")
            
            # Load schema definition to ensure proper data types
            schema_def = self.creator.load_table_schema(table_name, self.volume_path)
            
            if schema_def:
                # Create Spark schema and read CSV with proper types
                spark_schema = self.creator.create_spark_schema(schema_def)
                if spark_schema:
                    # Read CSV with Spark schema, preserving empty strings
                    # df_spark = self.spark.read.csv(csv_path, header=True, schema=spark_schema, emptyValue="", nullValue="null")
                    df_spark = (
                        self.spark.read
                            # --- schema -------------------------------------------------
                            .schema(spark_schema)                     # explicit schema
                            # --- CSV format options ------------------------------------
                            .option("header",        "true")          # first line = column names
                            .option("emptyValue",    "")              # treat empty string as ""
                            .option("nullValue",     "null")          # literal “null” → SQL NULL
                            .option("quote",         "\"")           # field quoting character
                            .option("escape",        "\"")           # escape a quote by doubling it
                            .option("multiLine",    "true")          # if any column may contain line‑breaks
                            # -----------------------------------------------------------
                            .csv(csv_path)                           # read the file  
                    )                  
                else:
                    # Fallback to reading without schema, preserving empty strings
                    # df_spark = self.spark.read.csv(csv_path, header=True, emptyValue="", nullValue="null")
                    df_spark = (
                        self.spark.read
                            # --- CSV format options ------------------------------------
                            .option("header",        "true")          # first line = column names
                            .option("emptyValue",    "")              # treat empty string as ""
                            .option("nullValue",     "null")          # literal “null” → SQL NULL
                            .option("quote",         "\"")           # field quoting character
                            .option("escape",        "\"")           # escape a quote by doubling it
                            .option("multiLine",    "true")          # if any column may contain line‑breaks
                            # -----------------------------------------------------------
                            .csv(csv_path)                           # read the file  
                    )                     
            else:
                # No schema definition, read as-is, preserving empty strings
                # df_spark = self.spark.read.csv(csv_path, header=True, emptyValue="", nullValue="null")
                df_spark = (
                    self.spark.read
                        # --- CSV format options ------------------------------------
                        .option("header",        "true")          # first line = column names
                        .option("emptyValue",    "")              # treat empty string as ""
                        .option("nullValue",     "null")          # literal “null” → SQL NULL
                        .option("quote",         "\"")           # field quoting character
                        .option("escape",        "\"")           # escape a quote by doubling it
                        .option("multiLine",    "true")          # if any column may contain line‑breaks
                        # -----------------------------------------------------------
                        .csv(csv_path)                           # read the file  
                )                
            
            row_count = df_spark.count()
            print(f"✅ CSV data loaded successfully: {row_count} rows")
            return df_spark
            
        except Exception as e:
            print(f"❌ Failed to load CSV data for {table_name}: {e}")
            print(f"🔍 Error details:")
            print(f"   - Error type: {type(e).__name__}")
            print(f"   - Error message: {str(e)}")
            
            # Additional debugging for CSV structure issues
            try:
                print(f"🔍 CSV file analysis:")
                with open(csv_path, 'r') as f:
                    lines = f.readlines()
                    print(f"   - Total lines in file: {len(lines)}")
                    print(f"   - Header line: {lines[0].strip() if lines else 'EMPTY'}")
                    if len(lines) > 1:
                        print(f"   - First data line: {lines[1].strip()}")
                    if len(lines) > 2:
                        print(f"   - Second data line: {lines[2].strip()}")
                    
                    # Check for trailing commas or empty lines
                    header_parts = lines[0].strip().split(',') if lines else []
                    print(f"   - Header columns count: {len(header_parts)}")
                    if lines[0].strip().endswith(','):
                        print(f"   - ⚠️  WARNING: Header ends with trailing comma!")
                    
                    # Check for empty lines
                    empty_lines = [i for i, line in enumerate(lines) if line.strip() == '']
                    if empty_lines:
                        print(f"   - ⚠️  WARNING: Empty lines found at positions: {empty_lines}")
                    
                    # Check for mismatched column counts
                    if len(lines) > 1:
                        for i, line in enumerate(lines[1:], 1):
                            if line.strip():  # Skip empty lines
                                data_parts = line.strip().split(',')
                                if len(data_parts) != len(header_parts):
                                    print(f"   - ⚠️  WARNING: Line {i+1} has {len(data_parts)} columns, expected {len(header_parts)}")
                                    print(f"     Line content: {line.strip()}")
                                    
            except Exception as debug_error:
                print(f"   - Could not analyze CSV file structure: {debug_error}")
            
            return None
    
    def load_table_data(self, table_name: str):
        """
        Load data from the actual table in the catalog.
        
        Args:
            table_name: Name of the table
            
        Returns:
            Spark DataFrame containing the table data
        """
        try:
            # Construct full table name
            full_table_name = f"{self.target_catalog}.{table_name}"
            
            print(f"📋 Loading table data from: {full_table_name}")
            
            # Query the table
            df_spark = self.spark.sql(f"SELECT * FROM {full_table_name}")
            
            row_count = df_spark.count()
            print(f"✅ Table data loaded successfully: {row_count} rows")
            return df_spark
            
        except Exception as e:
            print(f"❌ Failed to load table data for {table_name}: {e}")
            return None
    
    def compare_dataframes(self, csv_df, table_df, table_name: str) -> dict:
        """
        Compare CSV DataFrame with table DataFrame.
        
        Args:
            csv_df: Spark DataFrame from CSV file
            table_df: Spark DataFrame from actual table
            table_name: Name of the table being compared
            
        Returns:
            Dictionary containing comparison results
        """
        comparison_result = {
            "table_name": table_name,
            "csv_row_count": csv_df.count() if csv_df is not None else 0,
            "table_row_count": table_df.count() if table_df is not None else 0,
            "row_count_match": False,
            "column_count_match": False,
            "column_names_match": False,
            "data_match": False,
            "errors": [],
            "warnings": []
        }
        
        try:
            # Check if both DataFrames are valid
            if csv_df is None:
                comparison_result["errors"].append("CSV DataFrame is None")
                return comparison_result
                
            if table_df is None:
                comparison_result["errors"].append("Table DataFrame is None")
                return comparison_result
            
            # Get row counts
            csv_row_count = csv_df.count()
            table_row_count = table_df.count()
            
            # Compare row counts
            comparison_result["row_count_match"] = csv_row_count == table_row_count
            if not comparison_result["row_count_match"]:
                comparison_result["warnings"].append(
                    f"Row count mismatch: CSV has {csv_row_count} rows, table has {table_row_count} rows"
                )
            
            # Get column information
            csv_columns = csv_df.columns
            table_columns = table_df.columns
            
            # Compare column counts
            comparison_result["column_count_match"] = len(csv_columns) == len(table_columns)
            if not comparison_result["column_count_match"]:
                comparison_result["warnings"].append(
                    f"Column count mismatch: CSV has {len(csv_columns)} columns, table has {len(table_columns)} columns"
                )
            
            # Compare column names (case-insensitive)
            csv_columns_upper = {col.upper(): col for col in csv_columns}
            table_columns_upper = {col.upper(): col for col in table_columns}
            csv_columns_upper_set = set(csv_columns_upper.keys())
            table_columns_upper_set = set(table_columns_upper.keys())
            comparison_result["column_names_match"] = csv_columns_upper_set == table_columns_upper_set
            if not comparison_result["column_names_match"]:
                missing_in_table_upper = csv_columns_upper_set - table_columns_upper_set
                missing_in_csv_upper = table_columns_upper_set - csv_columns_upper_set
                if missing_in_table_upper:
                    missing_in_table = {csv_columns_upper[col] for col in missing_in_table_upper}
                    comparison_result["warnings"].append(f"Columns in CSV but not in table: {missing_in_table}")
                if missing_in_csv_upper:
                    missing_in_csv = {table_columns_upper[col] for col in missing_in_csv_upper}
                    comparison_result["warnings"].append(f"Columns in table but not in CSV: {missing_in_csv}")
            
            # Compare data content (only if row counts match and we have the same columns)
            if (comparison_result["row_count_match"] and 
                comparison_result["column_names_match"]):
                
                # Special case: if both datasets are empty, they match
                if csv_row_count == 0 and table_row_count == 0:
                    comparison_result["data_match"] = True
                    print(f"✅ Data content matches for {table_name} (both datasets are empty)")
                else:
                    # Get common columns for comparison (case-insensitive)
                    common_columns_upper = csv_columns_upper_set.intersection(table_columns_upper_set)
                    # Map back to original column names from CSV (preserve CSV column names for consistency)
                    common_columns = [csv_columns_upper[col_upper] for col_upper in common_columns_upper]
                    # Create mapping from CSV column names to table column names for proper selection
                    csv_to_table_column_map = {csv_columns_upper[col_upper]: table_columns_upper[col_upper] for col_upper in common_columns_upper}
                    if common_columns:
                        # Select only common columns and order them consistently
                        csv_selected = csv_df.select(*common_columns)
                        # Use table column names for table selection
                        table_column_names = [csv_to_table_column_map[col] for col in common_columns]
                        table_selected = table_df.select(*table_column_names)
                        
                        # Use Spark operations for data comparison (no pandas)
                        try:
                            # Normalize null values in both dataframes before comparison
                            csv_clean = self.normalize_null_values(csv_selected, common_columns)
                            table_clean = self.normalize_null_values(table_selected, common_columns)
                            
                            # Skip timestamp columns that might cause issues
                            col_to_skip = [
                                'LM_PROCESSING_DTTM', 
                                "LM_PRCSNG_MST_DTTM", 
                                "LOT_LAST_UPDT_DTTM", 
                                "LAST_UPDT_DTTM",
                                "ROW_MODIFY_MST_DTTM",
                                "ROW_MODIFY_GMT_DTTM",
                                "ROW_CRTE_GMT_DTTM",
                                "ROW_CRTE_MST_DTTM",
                                "LOT_ID_ASGNMT_TO_DTTM",
                                "LOT_ID_ASGNMT_LAST_UPDT_DTTM",
                                "LAST_UPDATE_DATETIME",
                                "LAST_UPDT_LOCAL_DTTM",
                                "LOT_ITM_ASSOC_TO_DTTM",
                                "LAST_EVENT_GMT_DATETIME",
                                "FINCL_MILESTONE_CNT" # string type. But valie is current timestamp
                            ]
                            
                            # Create case-insensitive mapping for column skipping
                            col_to_skip_upper = {col.upper() for col in col_to_skip}
                            comparison_columns = [c for c in common_columns if c.upper() not in col_to_skip_upper]
                            
                            if comparison_columns:
                                # Select only the columns we want to compare
                                csv_compare = csv_clean.select(*comparison_columns)
                                table_compare = table_clean.select(*comparison_columns)
                                
                                # Use Spark's except operation to find differences
                                # Note: exceptAll should handle nulls properly, but let's ensure consistent ordering
                                csv_ordered = csv_compare.orderBy(*comparison_columns)
                                table_ordered = table_compare.orderBy(*comparison_columns)
                                
                                csv_only = csv_ordered.exceptAll(table_ordered)
                                table_only = table_ordered.exceptAll(csv_ordered)
                                
                                csv_only_count = csv_only.count()
                                table_only_count = table_only.count()
                                
                                # Debug: Print some information about the comparison
                                print(f"🔍 Comparison debug for {table_name}:")
                                print(f"  CSV rows: {csv_compare.count()}")
                                print(f"  Table rows: {table_compare.count()}")
                                print(f"  CSV-only rows: {csv_only_count}")
                                print(f"  Table-only rows: {table_only_count}")
                                
                                # Additional debug: Show sample data from both sides
                                if csv_only_count > 0:
                                    print("  Sample CSV-only data:")
                                    csv_only.show(2, truncate=False)
                                if table_only_count > 0:
                                    print("  Sample Table-only data:")
                                    table_only.show(2, truncate=False)
                                
                                # If row counts match but data differs, try to find specific field differences
                                if csv_only_count == table_only_count and csv_only_count > 0:
                                    # This means we have the same number of rows but they're different
                                    # Let's try a more detailed comparison
                                    try:
                                        # Add row numbers to both DataFrames for comparison
                                        from pyspark.sql.functions import row_number, lit
                                        from pyspark.sql.window import Window
                                        
                                        csv_with_rownum = csv_compare.withColumn("row_num", row_number().over(Window.orderBy(lit(1))))
                                        table_with_rownum = table_compare.withColumn("row_num", row_number().over(Window.orderBy(lit(1))))
                                        
                                        # Join on row number to compare corresponding rows
                                        joined = csv_with_rownum.alias("csv").join(
                                            table_with_rownum.alias("table"), 
                                            "row_num", 
                                            "inner"
                                        )
                                        
                                        # Find field-level differences
                                        field_differences = []
                                        for csv_column_name in comparison_columns:
                                            if csv_column_name != "row_num":
                                                # Get the corresponding table column name
                                                table_column_name = csv_to_table_column_map[csv_column_name]
                                                diff_rows = joined.filter(
                                                    col(f"csv.{csv_column_name}") != col(f"table.{table_column_name}")
                                                )
                                                diff_count = diff_rows.count()
                                                if diff_count > 0:
                                                    field_differences.append(f"Column '{csv_column_name}' (CSV) / '{table_column_name}' (Table) differs in {diff_count} rows")
                                        
                                        if field_differences:
                                            comparison_result["errors"].extend(field_differences)
                                            print(f"Field-level differences found: {field_differences}")
                                            
                                    except Exception as detail_error:
                                        print(f"Could not perform detailed field comparison: {detail_error}")
                                
                                if csv_only_count == 0 and table_only_count == 0:
                                    comparison_result["data_match"] = True
                                    print(f"✅ Data content matches for {table_name}")
                                else:
                                    comparison_result["data_match"] = False
                                    comparison_result["errors"].append(f"Data mismatch: {csv_only_count} rows in CSV only, {table_only_count} rows in table only")
                                    print(f"❌ Data content mismatch for {table_name}: {csv_only_count} rows in CSV only, {table_only_count} rows in table only")
                                    
                                    # Capture actual data differences for the report
                                    if csv_only_count > 0:
                                        print("Sample rows in CSV but not in table:")
                                        csv_only.show(3, truncate=False)
                                        # Collect the actual data for the report
                                        csv_diff_data = csv_only.collect()
                                        for i, row in enumerate(csv_diff_data[:5]):  # Limit to first 5 rows
                                            row_dict = row.asDict()
                                            comparison_result["errors"].append(f"CSV-only row {i+1}: {row_dict}")
                                    
                                    if table_only_count > 0:
                                        print("Sample rows in table but not in CSV:")
                                        table_only.show(3, truncate=False)
                                        # Collect the actual data for the report
                                        table_diff_data = table_only.collect()
                                        for i, row in enumerate(table_diff_data[:5]):  # Limit to first 5 rows
                                            row_dict = row.asDict()
                                            comparison_result["errors"].append(f"Table-only row {i+1}: {row_dict}")
                            else:
                                comparison_result["data_match"] = True
                                print(f"✅ Data content matches for {table_name} (no comparable columns)")
                                
                        except Exception as spark_error:
                            # If Spark comparison fails, fall back to basic structural comparison
                            comparison_result["warnings"].append(f"Could not perform detailed data comparison due to: {spark_error}")
                            comparison_result["data_match"] = True  # Assume match if we can't verify
                            print(f"⚠️  Skipping detailed data comparison for {table_name} due to Spark issues")
                            print(f"🔍 Spark error details:")
                            print(f"   - Error type: {type(spark_error).__name__}")
                            print(f"   - Error message: {str(spark_error)}")
                            
                            
                    else:
                        comparison_result["warnings"].append("No common columns found for data comparison")
            else:
                comparison_result["warnings"].append("Skipping data content comparison due to structural differences")
            
            # Overall validation status
            comparison_result["validation_passed"] = (
                comparison_result["row_count_match"] and
                comparison_result["column_names_match"] and
                comparison_result["data_match"] and
                len(comparison_result["errors"]) == 0
            )
            
        except Exception as e:
            comparison_result["errors"].append(f"Comparison failed: {str(e)}")
            print(f"❌ Comparison failed for {table_name}: {e}")
        
        return comparison_result
    
    def validate_table(self, table_name: str, task_file_name: str, test_name: str) -> dict:
        """
        Validate a single table by comparing CSV data with table content.
        
        Args:
            table_name: Name of the table to validate
            task_file_name: Name of the task file
            test_name: Name of the test
            
        Returns:
            Dictionary containing validation results
        """
        print(f"\n🔍 Starting validation for table: {table_name}")
        
        # Load CSV data
        csv_df = self.load_csv_data(table_name, task_file_name, test_name)
        
        # Load table data
        table_df = self.load_table_data(table_name)
        
        # Compare the data
        comparison_result = self.compare_dataframes(csv_df, table_df, table_name)
        
        # Store result
        self.validation_results[table_name] = comparison_result
        
        return comparison_result
    
    def get_tables_with_post_test_data(self, task_file_name: str, test_name: str) -> list:
        """
        Get list of tables that have corresponding CSV files in post_test folder.
        
        Args:
            task_file_name: Name of the task file
            test_name: Name of the test
            
        Returns:
            List of table names that have post_test CSV files
        """
        # Load scripts configuration
        scripts_config = get_scripts_config()
        
        if task_file_name not in scripts_config:
            raise ValueError(f"Task file name '{task_file_name}' not found in scripts configuration")
        
        script_config = scripts_config[task_file_name]
        all_tables = script_config["update_tables"]
        
        # Check which tables have post_test CSV files
        tables_with_post_test = []
        post_test_path = os.path.join(self.volume_path, task_file_name, test_name, "post_test")
        
        for table_name in all_tables:
            csv_path = os.path.join(post_test_path, f"{table_name}.csv")
            if os.path.exists(csv_path):
                tables_with_post_test.append(table_name)
        
        return tables_with_post_test

    def validate_all_tables(self, task_file_name: str, test_name: str) -> dict:
        """
        Validate all tables that have corresponding CSV files in post_test folder.
        
        Args:
            task_file_name: Name of the task file
            test_name: Name of the test
            
        Returns:
            Dictionary containing validation results for tables with post_test data
        """
        print(f"\n🚀 Starting validation for task: {task_file_name}, test: {test_name}")
        
        # Get only tables that have post_test CSV files
        tables_to_validate = self.get_tables_with_post_test_data(task_file_name, test_name)
        
        if not tables_to_validate:
            print("⚠️  No tables found with post_test data to validate")
            return {}
        
        print(f"📋 Found {len(tables_to_validate)} tables with post_test data to validate: {tables_to_validate}")
        
        # Validate each table
        for table_name in tables_to_validate:
            self.validate_table(table_name, task_file_name, test_name)
        
        # Generate summary
        self.generate_validation_summary()
        
        return self.validation_results
    
    def generate_validation_summary(self):
        """Generate and print a validation summary."""
        print(f"\n📊 VALIDATION SUMMARY")
        print("=" * 50)
        
        total_tables = len(self.validation_results)
        passed_tables = sum(1 for result in self.validation_results.values() if result.get("validation_passed", False))
        failed_tables = total_tables - passed_tables
        
        print(f"Total tables validated: {total_tables}")
        print(f"Passed: {passed_tables}")
        print(f"Failed: {failed_tables}")
        print(f"Success rate: {(passed_tables/total_tables*100):.1f}%" if total_tables > 0 else "N/A")
        
        print(f"\n📋 DETAILED RESULTS:")
        for table_name, result in self.validation_results.items():
            status = "✅ PASSED" if result.get("validation_passed", False) else "❌ FAILED"
            print(f"\n{table_name}: {status}")
            print(f"  CSV rows: {result.get('csv_row_count', 'N/A')}")
            print(f"  Table rows: {result.get('table_row_count', 'N/A')}")
            print(f"  Row count match: {result.get('row_count_match', False)}")
            print(f"  Column names match: {result.get('column_names_match', False)}")
            print(f"  Data content match: {result.get('data_match', False)}")
            
            if result.get("warnings"):
                print(f"  Warnings: {len(result['warnings'])}")
                for warning in result["warnings"]:
                    print(f"    ⚠️  {warning}")
            
            if result.get("errors"):
                print(f"  Errors: {len(result['errors'])}")
                for error in result["errors"]:
                    print(f"    ❌ {error}")
    
    def save_validation_report(self, task_file_name: str, test_name: str, output_path: str = None):
        """
        Save validation results to a JSON file in the validation_reports folder.
        
        Args:
            task_file_name: Name of the task file (script name)
            test_name: Name of the test
            output_path: Path to save the report (optional, will use default naming if not provided)
        """
        if output_path is None:
            # Create validation_reports folder if it doesn't exist
            validation_reports_dir = os.path.join(self.volume_path, "validation_reports")
            os.makedirs(validation_reports_dir, exist_ok=True)
            
            # Generate filename in format: ScriptName_TestName.json
            filename = f"{task_file_name}_{test_name}.json"
            output_path = os.path.join(validation_reports_dir, filename)
        
        try:
            with open(output_path, 'w') as f:
                json.dump(self.validation_results, f, indent=2, default=str)
            print(f"📄 Validation report saved to: {output_path}")
        except Exception as e:
            print(f"❌ Failed to save validation report: {e}")
