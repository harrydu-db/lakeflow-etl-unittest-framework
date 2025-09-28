import os
import json
from pyspark.sql.types import *


class TableAndViewCreator:    
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

    def load_table_schema(self, table_name: str, volume_path: str) -> dict:
        """Load table schema from JSON definition file."""
        try:
            # Convert table name to lowercase for file lookup
            table_name_lower = table_name.lower()
            
            # Construct path to table definition JSON file using the provided volume_path
            schema_file_path = os.path.join(volume_path, "table_definitions", f"{table_name_lower}.json")
            
            # Read the JSON file
            with open(schema_file_path, 'r') as f:
                schema_def = json.load(f)
            
            return schema_def
        except Exception as e:
            print(f"❌ Failed to load schema for table {table_name}: {e}")
            return None
    
    def create_spark_schema(self, schema_def: dict) -> StructType:
        """Convert JSON schema definition to Spark StructType."""
        try:
            fields = []
            for col in schema_def["columns"]:
                col_name = col["name"]
                col_type_str = col["type"]
                nullable = col.get("nullable", True)
                
                # Map type strings to Spark types
                if col_type_str.startswith("StringType"):
                    # Handle StringType with collation like StringType('UTF8_LCASE_RTRIM')
                    import re
                    match = re.search(r"StringType\('([^']+)'\)", col_type_str)
                    if match:
                        collation = match.group(1)
                        spark_type = StringType(collation)
                    else:
                        spark_type = StringType()
                elif col_type_str.startswith("IntegerType"):
                    spark_type = IntegerType()
                elif col_type_str.startswith("LongType"):
                    spark_type = LongType()
                elif col_type_str.startswith("ShortType"):
                    spark_type = ShortType()
                elif col_type_str.startswith("ByteType"):
                    spark_type = ByteType()
                elif col_type_str.startswith("DoubleType"):
                    spark_type = DoubleType()
                elif col_type_str.startswith("FloatType"):
                    spark_type = FloatType()
                elif col_type_str.startswith("BooleanType"):
                    spark_type = BooleanType()
                elif col_type_str.startswith("DateType"):
                    spark_type = DateType()
                elif col_type_str.startswith("TimestampType"):
                    spark_type = TimestampType()
                elif col_type_str.startswith("DecimalType"):
                    # Extract precision and scale from DecimalType(18,4)
                    import re
                    match = re.search(r'DecimalType\((\d+),(\d+)\)', col_type_str)
                    if match:
                        precision = int(match.group(1))
                        scale = int(match.group(2))
                        spark_type = DecimalType(precision, scale)
                    else:
                        spark_type = DecimalType(10, 0)  # default
                else:
                    print(f"⚠️  Unknown type {col_type_str}, defaulting to StringType")
                    spark_type = StringType()
                
                fields.append(StructField(col_name, spark_type, nullable))
            
            return StructType(fields)
        except Exception as e:
            print(f"❌ Failed to create Spark schema: {e}")
            return None

    def create_view_from_sql(self, spark, view_name: str, target_catalog: str, volume_path: str) -> bool:
        """Create view from SQL definition file"""
        try:          
            print(f"📋 Creating view from definition file: {view_name}")

            # Construct path to view definition SQL file using the provided volume_path
            view_file_path = os.path.join(volume_path, "view_definitions", f"{view_name}.sql")
            
            # Read the SQL file
            with open(view_file_path, 'r') as f:
                create_view_stmt = f.read().strip()
            
            if not create_view_stmt:
                raise ValueError(f"View definition file is empty: {view_file_path}")
            
            # Set the catalog context and execute the SQL to create the view
            spark.sql(f"USE CATALOG {target_catalog}")
            spark.sql(create_view_stmt)
            
            # Verify the view was created successfully
            try:
                spark.sql(f"DESCRIBE {target_catalog}.{view_name}")
                print(f"✅ View created and verified successfully: {view_name}")
            except Exception as verify_error:
                print(f"⚠️  View created but verification failed: {view_name} - {verify_error}")
            
            return True
        except Exception as e:
            print(f"❌ Failed to create view {view_name}: {e}")
            return False
    
    def create_table_from_csv(self, spark, table_name: str, target_catalog: str, task_file_name: str, test_name: str, volume_path: str, definitions_volume: str = None) -> bool:
        """Create table by reading CSV files to DataFrame and saving as Delta table with proper schema."""
        try:
            target_table = f"{target_catalog}.{table_name}"
            print(f"Dropping table if exists: {target_table}")
            spark.sql(f"DROP TABLE IF EXISTS {target_table}")
            
            # Load table schema from JSON definition
            print(f"📋 Loading schema definition for table: {table_name}")
            # Use definitions_volume if provided, otherwise fall back to volume_path
            schema_volume = definitions_volume if definitions_volume else volume_path
            schema_def = self.load_table_schema(table_name, schema_volume)
            
            # Construct CSV path: [volume_path]/[script_name]/[test_name]/pre_test/[table_name].csv
            csv_path = os.path.join(volume_path, task_file_name, test_name, "pre_test", f"{table_name}.csv")
            
            # Check if the path exists and determine if it's a file or folder
            csv_exists = False
            is_folder_with_parts = False
            
            if os.path.exists(csv_path):
                if os.path.isfile(csv_path):
                    # Single CSV file (original test framework)
                    csv_exists = True
                    is_folder_with_parts = False
                    print(f"📁 Found single CSV file at: {csv_path}")
                elif os.path.isdir(csv_path):
                    # Check if it's a folder with part files (test-builder generated)
                    part_files = [f for f in os.listdir(csv_path) if f.startswith('part-') and f.endswith('.csv')]
                    if part_files:
                        csv_exists = True
                        is_folder_with_parts = True
                        print(f"📁 Found CSV folder with {len(part_files)} part files at: {csv_path}")
                    else:
                        print(f"📁 Folder exists but no part files found at: {csv_path}")
                else:
                    print(f"📁 Path exists but is neither file nor directory: {csv_path}")
            else:
                print(f"📁 No CSV file or folder found for table {table_name} at: {csv_path}")
            
            # Validate CSV structure if file/folder exists
            if csv_exists:
                if is_folder_with_parts:
                    print(f"🔍 Validating CSV structure for folder: {csv_path}")
                    # Find the first part file to validate structure
                    part_files = [f for f in os.listdir(csv_path) if f.startswith('part-') and f.endswith('.csv')]
                    if part_files:
                        first_part_file = os.path.join(csv_path, part_files[0])
                        csv_validation = self.validate_csv_structure(first_part_file)
                    else:
                        print(f"⚠️  No part files found in folder: {csv_path}")
                        csv_validation = {"is_valid": False, "issues": ["No part files found"], "warnings": [], "data_lines": 0, "header_columns": 0}
                else:
                    print(f"🔍 Validating CSV structure for file: {csv_path}")
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
            
            if schema_def:
                print(f"✅ Schema definition loaded for {table_name}")
                
                # Create Spark schema from definition
                spark_schema = self.create_spark_schema(schema_def)
                if spark_schema:
                    if csv_exists:
                        if is_folder_with_parts:
                            print(f"📋 Reading CSV files from folder: {csv_path}")
                        else:
                            print(f"📋 Reading CSV file: {csv_path}")
                        print(f"📄 Reading CSV with defined schema")
                        # In modern Spark (2.4+), empty strings are preserved by default
                        df = (
                            spark.read
                                # --- schema -------------------------------------------------
                                .schema(spark_schema)                     # explicit schema
                                # --- CSV format options ------------------------------------
                                .option("header",        "true")          # first line = column names
                                .option("nullValue",     "null")          # literal "null" → SQL NULL
                                .option("quote",         "\"")           # field quoting character
                                .option("escape",        "\"")           # escape a quote by doubling it
                                .option("multiLine",    "true")          # if any column may contain line‑breaks
                                # -----------------------------------------------------------
                                .csv(csv_path)                           # read file or folder containing part files
                        )                           
                    else:
                        print(f"📋 CSV file/folder not found for: {table_name}")
                        print(f"📋 Will create empty table with schema for: {table_name}")
                        print(f"📄 Creating empty DataFrame with defined schema")
                        # Create empty DataFrame with the schema
                        df = spark.createDataFrame([], spark_schema)
                else:
                    if csv_exists:
                        if is_folder_with_parts:
                            print(f"📋 Reading CSV files from folder: {csv_path}")
                        else:
                            print(f"📋 Reading CSV file: {csv_path}")
                        print(f"⚠️ Failed to create Spark schema for {table_name}, reading CSV without schema")
                        # In modern Spark (2.4+), empty strings are preserved by default
                        df = (
                            spark.read
                                # --- CSV format options ------------------------------------
                                .option("header",        "true")          # first line = column names
                                .option("nullValue",     "null")          # literal "null" → SQL NULL
                                .option("quote",         "\"")           # field quoting character
                                .option("escape",        "\"")           # escape a quote by doubling it
                                .option("multiLine",    "true")          # if any column may contain line‑breaks
                                # -----------------------------------------------------------
                                .csv(csv_path)                           # read file or folder containing part files
                        )                          
                    else:
                        print(f"📋 CSV file/folder not found for: {table_name}")
                        print(f"❌ Cannot create empty table without valid schema for {table_name}")
                        return False
            else:
                if csv_exists:
                    if is_folder_with_parts:
                        print(f"📋 Reading CSV files from folder: {csv_path}")
                    else:
                        print(f"📋 Reading CSV file: {csv_path}")
                    print(f"⚠️ No schema definition found for table: {table_name}, reading CSV without schema")
                    # In modern Spark (3.7+), empty strings are preserved by default
                    df = (
                        spark.read
                            # --- CSV format options ------------------------------------
                            .option("header",        "true")          # first line = column names
                            .option("nullValue",     "null")          # literal "null" → SQL NULL
                            .option("quote",         "\"")           # field quoting character
                            .option("escape",        "\"")           # escape a quote by doubling it
                            .option("multiLine",    "true")          # if any column may contain line‑breaks
                            # -----------------------------------------------------------
                            .csv(csv_path)                           # read file or folder containing part files
                    )                        
                else:
                    print(f"📋 CSV file/folder not found for: {table_name}")
                    print(f"❌ Cannot create empty table without schema definition for {table_name}")
                    return False
            
            print(f"📄 Writing DataFrame to table: {target_table} (format: delta, mode: overwrite)")
            
            # Create table with allowColumnDefaults and catalogOwned-preview features enabled
            (df.write.format("delta").mode("overwrite")
            .option("delta.feature.allowColumnDefaults", "supported")
            .option("delta.feature.catalogOwned-preview", "supported")
            .saveAsTable(target_table))
            print(f"✅ Table created successfully: {target_table}")
            return True
        except Exception as e:
            print(f"❌ Failed to create table {target_table}: {e}")
            return False

    def prepare_catalog_and_schemas(self, spark, catalog_name: str, schemas: list = None) -> bool:
        """
        Check if the target catalog exists, create it if it doesn't, and create all required schemas.
        
        Args:
            spark: Spark session
            catalog_name: Name of the catalog to create/prepare
            schemas: List of schema names to create (optional, uses default if not provided)
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            print(f"🔍 Checking and preparing catalog: {catalog_name}")
            
            # Check if catalog exists
            try:
                spark.sql(f"DESCRIBE CATALOG {catalog_name}")
                print(f"✅ Catalog {catalog_name} already exists")
            except Exception:
                print(f"📦 Creating catalog: {catalog_name}")
                spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_name}")
                print(f"✅ Catalog {catalog_name} created successfully")
            
            # Set the catalog context
            spark.sql(f"USE CATALOG {catalog_name}")
            
            # Use provided schemas or default list
            if schemas is None or len(schemas) == 0:
                return True
            
            # Create each schema if it doesn't exist
            for schema in schemas:
                schema_full_name = f"{catalog_name}.{schema}"
                try:
                    print(f"📁 Creating schema: {schema_full_name}")
                    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_full_name}")
                    print(f"✅ Schema {schema_full_name} created successfully")
                except Exception as e:
                    print(f"❌ Failed to create schema {schema_full_name}: {e}")
                    return False
            
            print(f"✅ All schemas prepared successfully for catalog {catalog_name}")
            return True
            
        except Exception as e:
            print(f"❌ Failed to prepare catalog and schemas for {catalog_name}: {e}")
            return False

    def create_tables_and_views(self, spark, scripts_config: dict, task_file_name: str, test_name: str, target_catalog: str, volume_path: str, definitions_volume: str = None, common_tables: list = None) -> bool:
        """Create tables and views by reading CSV files and SQL definitions."""
        try:
            
            # Get the script configuration for the current task_file_name
            if task_file_name not in scripts_config:
                raise ValueError(f"Task file name '{task_file_name}' not found in scripts configuration")
            
            script_config = scripts_config[task_file_name]
            
            all_created_successfully = True
            
            # Process tables
            tables = script_config["tables"]
            # Add common additional tables
            if common_tables:
                tables.extend(common_tables)
            for table_name in tables:
                rtn = self.create_table_from_csv(spark, table_name, target_catalog, task_file_name, test_name, volume_path, definitions_volume)
                if not rtn:
                    all_created_successfully = False
                    
            # Process views in the order specified in configuration
            # Use definitions_volume for view definitions if provided, otherwise use volume_path
            view_volume = definitions_volume if definitions_volume else volume_path
            for view_name in script_config["views"]:
                target_view = f"{target_catalog}.{view_name}"
                print(f"Dropping view if exists: {target_view}")
                try:
                    spark.sql(f"DROP VIEW IF EXISTS {target_view}")
                except Exception as e:
                    print(f"Failed to drop as view, trying as table: {e}")
                    spark.sql(f"DROP TABLE IF EXISTS {target_view}")
                    print(f"Successfully dropped as table: {target_view}")

                rtn = self.create_view_from_sql(spark, view_name, target_catalog, view_volume)
                if not rtn:
                    all_created_successfully = False
                    
            return all_created_successfully
        except Exception as e:
            print(f"❌ Failed to create tables and views: {e}")
            return False
