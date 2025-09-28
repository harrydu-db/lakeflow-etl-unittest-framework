
import os
import json
from pyspark.sql import SparkSession



class TestBuilder:
    """Test builder that automatically generates test data for the testing framework."""
    
    def __init__(self, config_path: str, script_metadata_path: str):
        """
        Initialize the TestBuilder.
        
        Args:
            config_path: Path to config.json file
            script_metadata_path: Path to script_metadata.json file
        """
        self.config_path = config_path
        self.script_metadata_path = script_metadata_path
        self.spark = SparkSession.getActiveSession()
        self.spark.conf.set("spark.sql.session.timeZone", "America/Phoenix")
        
        # Load configurations
        self.config = self._load_config()
        self.script_metadata = self._load_script_metadata()
        
        # Extract values from config
        self.output_volume = self.config["output_volume"]
        self.target_catalog = self.config["target_catalog"]
    
    def _load_config(self) -> dict:
        """Load configuration from config.json file."""
        try:
            with open(self.config_path, 'r') as f:
                config = json.load(f)
            print(f"✅ Loaded config from {self.config_path}")
            return config
        except Exception as e:
            print(f"❌ Failed to load config: {e}")
            raise
    
    def _load_script_metadata(self) -> dict:
        """Load script metadata from script_metadata.json file."""
        try:
            with open(self.script_metadata_path, 'r') as f:
                metadata = json.load(f)
            print(f"✅ Loaded script metadata from {self.script_metadata_path}")
            return metadata
        except Exception as e:
            print(f"❌ Failed to load script metadata: {e}")
            raise
    
    def get_test_config(self, script_name: str, test_name: str) -> dict:
        """
        Get test configuration from config.json for a specific test.
        
        Args:
            script_name: Name of the script to get configuration for
            test_name: Name of the test to get configuration for
            
        Returns:
            Dictionary containing test configuration or None if not found
        """
        tests = self.config.get("tests", [])
        for test_config in tests:
            if test_config.get("script") == script_name and test_config.get("test_name") == test_name:
                return test_config
        return None
    
    def _ensure_directory_exists(self, directory_path: str):
        """Ensure directory exists, create if it doesn't."""
        try:
            os.makedirs(directory_path, exist_ok=True)
            print(f"✅ Directory ensured: {directory_path}")
        except Exception as e:
            print(f"❌ Failed to create directory {directory_path}: {e}")
            raise
    

    def _build_filter_condition(self, table_name: str, resolved_filters: dict, filter_exp: str = None) -> str:
        """
        Build filter condition for a table based on resolved filter values or filter expression.
        
        Args:
            table_name: Full table name (catalog.schema.table)
            resolved_filters: Dictionary of resolved filter values (includes mapped values)
            filter_exp: Filter expression with placeholders like {LOT_ID}
            
        Returns:
            WHERE clause string or None if no applicable filters
        """
        try:
            # If filter_exp is provided, use it instead of individual column filters
            if filter_exp:
                resolved_filter_exp = self._resolve_filter_expression(filter_exp, resolved_filters)
                if resolved_filter_exp:
                    print(f"    🔍 Using resolved filter expression: {resolved_filter_exp}")
                    return resolved_filter_exp
                else:
                    print(f"    ⚠️  Could not resolve filter expression: {filter_exp}")
            
            # Fall back to individual column filters
            # Get table schema to check which columns exist
            df = self.spark.sql(f"SELECT * FROM {table_name} LIMIT 0")
            available_columns = [field.name for field in df.schema.fields]
            
            applicable_filters = []
            # Create a case-insensitive mapping of available columns
            available_columns_upper = {col.upper(): col for col in available_columns}
            
            for column_name, value in resolved_filters.items():
                # Check for case-insensitive match
                matched_column = available_columns_upper.get(column_name.upper())
                if matched_column:
                    # Handle None values properly in SQL - use IS NULL instead of = 'None'
                    if value is None:
                        applicable_filters.append(f"{matched_column} IS NULL")
                        print(f"    🔍 Applying filter: {matched_column} IS NULL")
                    else:
                        applicable_filters.append(f"{matched_column} = '{value}'")
                        print(f"    🔍 Applying filter: {matched_column} = '{value}'")
            if applicable_filters:
                return " AND ".join(applicable_filters)
            else:
                print(f"    ℹ️  No applicable filters for {table_name}")
                return None
                
        except Exception as e:
            print(f"    ❌ Failed to check columns for {table_name}: {e}")
            return None


    def _dump_table_data(self, table_name: str, output_path: str, resolved_filters: dict, filter_exp: str = None):
        """
        Dump table data to CSV file using resolved filters.
        
        Args:
            table_name: Full table name (catalog.schema.table)
            output_path: Output CSV file path
            resolved_filters: Dictionary of resolved filter values (includes mapped values)
            filter_exp: Filter expression with placeholders like {LOT_ID}
        """
        try:
            print(f"📋 Dumping data from table: {table_name}")
            
            # Build filter condition for this table
            filter_condition = self._build_filter_condition(table_name, resolved_filters, filter_exp)
            
            # Build query with resolved filter condition
            if filter_condition:
                query = f"SELECT * FROM {table_name} WHERE {filter_condition}"
                print(f"  🔍 Using filter condition: {filter_condition}")
            else:
                query = f"SELECT * FROM {table_name} LIMIT 1"
            
            df = self.spark.sql(query)
            
            # # Clean the output directory before writing using os
            # # Only clear if output_path is a directory and exists
            # try:
            #     if os.path.isdir(output_path) and os.path.exists(output_path):
            #         import shutil
            #         shutil.rmtree(output_path)
            #         print(f"  🧹 Cleaned output directory: {output_path}")
            # except Exception as e:
            #     print(f"  ⚠️  Could not clean directory {output_path}: {e}")
            #     # Continue anyway as the directory might not exist yet
            
            # Write to CSV directory (will be fixed during download)
            (df.coalesce(1)
               .write
               .mode("overwrite")
               .option("header", "true")
               .option("nullValue", "null")
               .option("quote", "\"")
               .option("escape", "\"")
               .csv(output_path))
            
            print(f"✅ Successfully dumped {df.count()} rows to {output_path}")
            
        except Exception as e:
            print(f"❌ Failed to dump data from table {table_name}: {e}")
            # Don't raise exception, just log the error and continue
            print(f"⚠️  Continuing with other tables...")

    def build_filters(self, script_name: str, test_name: str) -> dict:
        """
        Resolve filter chain for a script by getting filter_resolution and filter_mapping from config.
        This filters will be used to reduced the data to be dumped.
        
        Args:
            script_name: Name of the script to resolve filters for
            test_name: Name of the test to resolve filters for
        
        Returns:
            Dictionary of resolved filter values {column_name: value}
        """
        # Get script configuration from config.json
        script_config = self.get_test_config(script_name, test_name)
        if not script_config:
            print(f"❌ Script {script_name} not found in config.json")
            return {}
        
        # Extract filter data from config
        filter_resolution = script_config.get("filter_resolution", [])
        filter_mapping = script_config.get("filter_mapping", {})
        
        return self._resolve_filters_from_chain_data(filter_resolution, filter_mapping)
    
    def _resolve_filters_from_chain_data(self, filter_resolution: list, filter_mapping: dict = None) -> dict:
        """
        Resolve filter chain from the provided filter_resolution list and filter_mapping.
        
        Args:
            filter_resolution: List of filter resolution steps with new format:
                - "table": table name (for table-based resolution)
                - "columns": list of columns to extract values from
                - "filter_exp": (optional) filter expression with placeholders like {LOT_ID}
                - "lookup_columns": (optional) list of columns to use for filtering
                - "sql": (optional) direct SQL query to execute for filter values
            filter_mapping: Dictionary for field mapping (e.g., {"ENTERPRISE_LOT_ID": "LOT_ID"})
        
        Returns:
            Dictionary of resolved filter values {column_name: value}
        """
        if not filter_resolution:
            print(f"⚠️  No filter resolution path provided")
            return {}
        
        print(f"🔗 Resolving filter chain")
        resolved_filters = {}
        
        for i, step in enumerate(filter_resolution, 1):
            columns = step["columns"]
            
            # Check if this step uses SQL-based resolution
            if "sql" in step:
                sql_query = step["sql"]
                print(f"  Step {i}: Executing SQL query to get {columns}")
                print(f"    🔍 SQL: {sql_query}")
                
                try:
                    # Execute the SQL query directly
                    df = self.spark.sql(sql_query)
                    
                    if df.count() > 0:
                        row = df.collect()[0]
                        # Extract values for all specified columns
                        for col in columns:
                            value = row[col]
                            # Handle None values properly - don't convert to string 'None'
                            if value is None:
                                resolved_filters[col] = None
                                print(f"    ✅ Found {col} = None")
                            else:
                                resolved_filters[col] = str(value)
                                print(f"    ✅ Found {col} = {value}")
                    else:
                        print(f"    ⚠️  No data found from SQL query")
                        break
                        
                except Exception as e:
                    print(f"    ❌ Failed to execute SQL query: {e}")
                    break
                    
            else:
                # Original table-based resolution logic
                table_name = step["table"]
                filter_exp = step.get("filter_exp", "")
                lookup_columns = step.get("lookup_columns", [])
                
                print(f"  Step {i}: Getting {columns} from {table_name}")
                if filter_exp:
                    print(f"    🔍 Using filter expression: {filter_exp}")
                if lookup_columns:
                    print(f"    🔍 Using lookup columns: {lookup_columns}")
                
                try:
                    # Build query with filter expression or lookup filters
                    where_conditions = []
                    
                    if filter_exp:
                        # Use filter expression with placeholder substitution
                        resolved_filter_exp = self._resolve_filter_expression(filter_exp, resolved_filters)
                        if resolved_filter_exp:
                            where_conditions.append(resolved_filter_exp)
                    elif lookup_columns:
                        # Use lookup_columns to filter the table
                        for lookup_col in lookup_columns:
                            if lookup_col in resolved_filters:
                                value = resolved_filters[lookup_col]
                                # Handle None values properly in SQL - use IS NULL instead of = 'None'
                                if value is None:
                                    where_conditions.append(f"{lookup_col} IS NULL")
                                else:
                                    where_conditions.append(f"{lookup_col} = '{value}'")
                            else:
                                print(f"    ⚠️  Lookup column {lookup_col} not found in resolved filters")
                                break
                        else:
                            # All lookup columns found, proceed with query
                            pass
                    
                    # Build the query
                    if where_conditions:
                        where_clause = " AND ".join(where_conditions)
                        query = f"SELECT {', '.join(columns)} FROM {table_name} WHERE {where_clause} LIMIT 1"
                        print(f"    🔍 Query: {query}")
                    else:
                        query = f"SELECT {', '.join(columns)} FROM {table_name} LIMIT 1"
                        print(f"    🔍 Query: {query}")
                    
                    df = self.spark.sql(query)
                    
                    if df.count() > 0:
                        row = df.collect()[0]
                        # Extract values for all specified columns
                        for col in columns:
                            value = row[col]
                            # Handle None values properly - don't convert to string 'None'
                            if value is None:
                                resolved_filters[col] = None
                                print(f"    ✅ Found {col} = None")
                            else:
                                resolved_filters[col] = str(value)
                                print(f"    ✅ Found {col} = {value}")
                    else:
                        print(f"    ⚠️  No data found in {table_name}")
                        break
                        
                except Exception as e:
                    print(f"    ❌ Failed to resolve columns from {table_name}: {e}")
                    break
        
        # Add mapped values from filter_mapping to resolved_filters
        if filter_mapping:
            for mapped_key, source_key in filter_mapping.items():
                if source_key in resolved_filters:
                    resolved_filters[mapped_key] = resolved_filters[source_key]
                    print(f"    🔗 Added mapped value: {mapped_key} = {resolved_filters[source_key]} (from {source_key})")
                else:
                    print(f"    ⚠️  Source key {source_key} not found for mapping {mapped_key}")
        
        print(f"🎯 Resolved filters: {resolved_filters}")
        return resolved_filters
    

    def dump_posttest_tables(self, script_name: str, test_name: str):
        """
        Dump all update_tables as-is to CSV without using any filters. We run the ETL job in the unit test catalog using the reduced data.
        
        Args:
            script_name: Name of the script to process
            test_name: Name of the test
        """
        if script_name not in self.script_metadata:
            print(f"❌ Script {script_name} not found in metadata")
            return False
        
        script_config = self.script_metadata[script_name]
        update_tables = script_config.get("update_tables", [])
        
        if not update_tables:
            print(f"⚠️  No update tables found for script {script_name}")
            return False
        
        print(f"📋 Found {len(update_tables)} update table(s) to dump without filters")
        
        # Create post_test directory structure: [output_volume]/[script_name]/[test_name]/post_test
        output_dir = os.path.join(self.output_volume, script_name, test_name, "post_test")
        
        # Ensure output directory exists
        self._ensure_directory_exists(output_dir)
        
        success_count = 0
        # Loop through each update table
        for i, table_name in enumerate(update_tables, 1):
            print(f"\n  📋 Processing table {i}/{len(update_tables)}: {table_name}")
            
            # Create output file path
            output_file = os.path.join(output_dir, f"{table_name}.csv")
            
            # Dump table data without any filters
            try:
                print(f"📋 Dumping data from table: {table_name}")
                
                # Simple query without any filters
                query = f"SELECT * FROM {table_name}"
                df = self.spark.sql(query)
                
                # Write to CSV directory
                (df.coalesce(1)
                   .write
                   .mode("overwrite")
                   .option("header", "true")
                   .option("nullValue", "null")
                   .option("quote", "\"")
                   .option("escape", "\"")
                   .csv(output_file))
                
                row_count = df.count()
                print(f"✅ Successfully dumped {row_count} rows to {output_file}")
                success_count += 1
                
            except Exception as e:
                print(f"❌ Failed to dump data from table {table_name}: {e}")
                print(f"⚠️  Continuing with other tables...")
        
        print(f"✅ Successfully dumped {success_count}/{len(update_tables)} update tables")
        return success_count > 0

    def dump_pretest_tables(self, script_name: str, resolved_filters: dict, test_name: str):
        """
        Dump reference tables for a script using resolved filters.
        
        Args:
            script_name: Name of the script to process
            resolved_filters: Dictionary of resolved filter values
            test_name: Name of the test
        """
        if script_name not in self.script_metadata:
            print(f"❌ Script {script_name} not found in metadata")
            return False
        
        reference_tables = self.script_metadata[script_name].get("reference_tables", [])
        if not reference_tables:
            print(f"⚠️  No reference tables found for script {script_name}")
            return False
        
        print(f"📋 Found {len(reference_tables)} reference table(s) to dump")
        
        # Create pre_test directory structure: [output_volume]/[script_name]/[test_name]/pre_test
        output_dir = os.path.join(self.output_volume, script_name, test_name, "pre_test")
        
        # Ensure output directory exists
        self._ensure_directory_exists(output_dir)
        
        success_count = 0
        # Loop through each reference table
        for i, table_name in enumerate(reference_tables, 1):
            print(f"\n  📋 Processing table {i}/{len(reference_tables)}: {table_name}")
            
            # Create output file path
            output_file = os.path.join(output_dir, f"{table_name}.csv")
            
            # Dump table data using resolved filters
            try:
                self._dump_table_data(table_name, output_file, resolved_filters)
                success_count += 1
            except Exception as e:
                print(f"❌ Failed to dump data from table {table_name}: {e}")
                print(f"⚠️  Continuing with other tables...")
        
        print(f"✅ Successfully dumped {success_count}/{len(reference_tables)} reference tables")
        return success_count > 0

    def _resolve_filter_expression(self, filter_exp: str, resolved_filters: dict) -> str:
        """
        Resolve filter expression by substituting placeholders with actual values.
        
        Args:
            filter_exp: Filter expression with placeholders like {LOT_ID}
            resolved_filters: Dictionary of resolved filter values (includes mapped values)
        
        Returns:
            Resolved filter expression or None if placeholders cannot be resolved
        """
        if not filter_exp:
            return None
            
        try:
            import re
            
            # Find all placeholders in the format {PLACEHOLDER}
            placeholders = re.findall(r'\{([^}]+)\}', filter_exp)
            print(f"    🔍 Found placeholders: {placeholders}")
            
            resolved_exp = filter_exp
            
            for placeholder in placeholders:
                # Find the value directly in resolved_filters (includes mapped values)
                if placeholder in resolved_filters:
                    value = resolved_filters[placeholder]
                    resolved_exp = resolved_exp.replace(f"{{{placeholder}}}", f"{value}")
                    print(f"    ✅ Resolved {placeholder} = {value}")
                else:
                    print(f"    ⚠️  Placeholder {placeholder} not found in resolved filters")
                    return None
            
            print(f"    ✅ Resolved filter expression: {resolved_exp}")
            return resolved_exp
            
        except Exception as e:
            print(f"    ❌ Failed to resolve filter expression: {e}")
            return None

