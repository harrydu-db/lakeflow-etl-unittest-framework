# ETL Test Data Builder

The ETL Test Data Builder automatically generates test data that can be used by the testing framework. It follows the same patterns as the test-framework components and creates a complete unit test environment for ETL job validation.

## Overview

The Test Builder performs the following 4-step process for each enabled test in the configuration:

1. **Dump Pre-Test Data**: 
   - Resolves filter chains to identify specific data records
   - Dumps filtered reference tables from the target catalog
   - Runs addon system to create custom tables and configurations
   - Saves all data to the output volume in `pre_test` folder

2. **Prepare Unit Test Environment**: 
   - Sets up tables and views in the unit test catalog using the test-framework DataPrep
   - Loads the pre-test data into the unit test environment

3. **Run Unit Test**: 
   - Executes the actual ETL job against the unit test catalog
   - Uses the reduced dataset for testing

4. **Dump Post-Test Data**: 
   - Dumps all update tables from the unit test catalog (without filters)
   - Saves data to the output volume in `post_test` folder

## Files

- `TestBuilder.py`: Main test builder class with all the logic for filter resolution, data dumping, and test execution
- `TestBuilderRunner.py`: Main runner notebook that orchestrates the 4-step process for all enabled tests
- `DumpPreTestData.py`: Notebook for dumping pre-test data (reference tables and custom tables)
- `DumpPostTestData.py`: Notebook for dumping post-test data (update tables)
- `TestBuilderAddon.py`: Addon system for extending TestBuilder functionality without modifying core code
- `config.json`: Configuration file specifying which tests to process and their filter chains
- `script_metadata.json`: Metadata file containing table information for each script (See tests/utils/build_test_metadata.py for creating this file.)
- `README.md`: This documentation file

## Configuration

### config.json

```json
{
    "output_volume": "/Volumes/etl_unit_test/default/test_data_generated",
    "definitions_volume": "/Volumes/etl_unit_test/default/test_data",
    "target_catalog": "source_catalog",
    "unit_test_catalog": "etl_unit_test",
    "tests": [
        {
            "enabled": true,
            "test_name": "test_basic",
            "folder": "category",
            "script": "EXAMPLE_ETL_SCRIPT",
            "custom_config": {
                "PROCESS_NAME": "DATA_TRANSFORM",
                "SOURCE_SYSTEM": "SYSTEM_A",
                "PROCESSING_STATUS": "ACTIVE"
            },
            "filter_mapping": {
                "BUSINESS_KEY": "RECORD_ID"
            },
            "filter_resolution": [
                {
                    "sql": "SELECT DISTINCT B.RECORD_KEY, A.RECORD_ID, A.TRANSACTION_ID FROM SCHEMA1.VIEW_NAME as A...",
                    "columns": ["TRANSACTION_ID", "RECORD_ID", "RECORD_KEY"]
                }
            ]
        }
    ]
}
```

- `output_volume`: Volume path where test data will be saved
- `definitions_volume`: Volume path containing table/view definitions
- `target_catalog`: Source catalog for extracting test data
- `unit_test_catalog`: Target catalog for running unit tests
- `tests`: Array of test configurations
  - `enabled`: Whether this test should be processed
  - `test_name`: Name of the test (e.g., 'test_basic', 'test_advanced')
  - `folder`: Folder name (e.g., 'category1', 'category2', etc.)
  - `script`: Script name (e.g., 'EXAMPLE_ETL_SCRIPT')
  - `custom_config`: Configuration for addon-generated custom tables and logic
  - `filter_mapping`: Field mapping for filter resolution
  - `filter_resolution`: Chain of steps to resolve test data filters

### script_metadata.json

This file contains comprehensive metadata for each ETL script, generated from lineage analysis to support automated test data generation and testing framework.

#### Creation Process

The `script_metadata.json` file is generated through a two-step process:

1. **Lineage Analysis**: The [lineage-analyzer](https://github.com/harrydu-db/lineage-analyzer/) project analyzes ETL jobs in the source directory and generates lineage JSON files
2. **Metadata Generation**: The `build_test_metadata.py` utility processes the lineage JSON files to create comprehensive test metadata

To regenerate the metadata from updated lineage files:

```bash
# Generate script metadata from lineage files
python tests/utils/build_test_metadata.py
```

The pre-generated `script_metadata.json` file is already included and ready to use.

#### File Structure

The `script_metadata.json` file contains a dictionary where each key is a script name and the value is a metadata object with the following structure:

```json
{
  "SCRIPT_NAME": {
    "tables": ["TABLE1", "TABLE2", ...],
    "views": ["VIEW1", "VIEW2", ...],
    "update_tables": ["UPDATE_TABLE1", "UPDATE_TABLE2", ...],
    "reference_tables": ["REF_TABLE1", "REF_TABLE2", ...]
  }
}
```

#### Field Descriptions

- **`tables`**: All non-volatile tables used by the script, sorted alphabetically
  - Includes both reference and update tables
  - Excludes volatile tables (temporary tables, staging tables, etc.)
  - Used for comprehensive dependency tracking

- **`views`**: All non-volatile views used by the script, sorted by dependency order
  - Uses topological sorting to ensure proper creation order

- **`update_tables`**: Tables that are modified/updated by the script
  - Identified by having incoming dependencies from other tables/views
  - These tables are dumped after ETL job execution (post-test data)
  - Used to capture the results of the ETL job

- **`reference_tables`**: Tables used as data sources by the script
  - Includes both direct table references and base tables that views depend on
  - These tables are dumped before ETL job execution (pre-test data)
  - Used to provide input data for the ETL job

#### Dependency Resolution

The metadata generation process uses sophisticated dependency analysis:

1. **Lineage Processing**: Analyzes lineage JSON files to build dependency graphs
2. **Volatility Filtering**: Excludes volatile tables/views from test data
3. **View Dependencies**: Recursively finds base tables that views depend on
4. **Topological Sorting**: Ensures views are created in proper dependency order
5. **Update Detection**: Identifies which tables are being modified vs referenced

#### Example Entry

```json
{
  "EXAMPLE_ETL_SCRIPT": {
    "tables": ["SCHEMA1.TABLE1", "SCHEMA2.TABLE2", "SCHEMA3.TABLE3"],
    "views": ["SCHEMA1.VIEW1"],
    "update_tables": ["SCHEMA2.TABLE2"],
    "reference_tables": ["SCHEMA1.TABLE1", "SCHEMA3.TABLE3"]
  }
}
```

This indicates that the `EXAMPLE_ETL_SCRIPT` script:
- Uses 3 tables and 1 view
- Updates the `SCHEMA2.TABLE2` table
- References `SCHEMA1.TABLE1` and `SCHEMA3.TABLE3` as input data
- The view `SCHEMA1.VIEW1` is created first due to dependency ordering

## Usage

### Automatic Deployment (Recommended)

The test builder job will be deployed automatically when you deploy the asset bundle:

```bash
databricks bundle deploy -t dev -p your_profile
```

This will deploy all configured jobs including the test builder, making it available in your Databricks workspace without manual setup.

### Manual Execution Methods

#### Method 1: Using the main runner

1. Configure `config.json` with your test settings
2. Run the `TestBuilderRunner.py` notebook in Databricks
3. The runner will process all enabled tests automatically

#### Method 2: Individual step execution

You can run individual steps using the separate notebooks:

1. **Dump Pre-Test Data**: Run `DumpPreTestData.py` with widgets:
   - `script_name`: Name of the script to process
   - `test_name`: Name of the test

2. **Prepare Unit Test Environment**: Run the test-framework `DataPrep` notebook

3. **Run Unit Test**: Execute the actual ETL job notebook

4. **Dump Post-Test Data**: Run `DumpPostTestData.py` with widgets:
   - `target_catalog`: Unit test catalog name
   - `output_volume`: Output volume path
   - `script_name`: Script name
   - `folder`: Script folder
   - `test_name`: Test name

#### Method 3: Direct TestBuilder class usage

```python
from TestBuilder import TestBuilder
from TestBuilderAddon import TestBuilderAddon

# Initialize
test_builder = TestBuilder(
    config_path="config.json",
    script_metadata_path="script_metadata.json"
)

# Resolve filters for a specific test
resolved_filters = test_builder.build_filters("EXAMPLE_ETL_SCRIPT", "test_basic")

# Dump pre-test data
test_builder.dump_pretest_tables("EXAMPLE_ETL_SCRIPT", resolved_filters, "test_basic")

# Run addon (creates custom tables and configurations)
TestBuilderAddon.run_addon(test_builder, "EXAMPLE_ETL_SCRIPT", "test_basic")

# Dump post-test data
test_builder.dump_posttest_tables("EXAMPLE_ETL_SCRIPT", "test_basic")
```

## Output Structure

The test data is saved in the following structure:

```
output_volume/
├── SCRIPT_NAME/
│   └── test_name/
│       ├── pre_test/
│       │   ├── REFERENCE_TABLE1.csv
│       │   ├── REFERENCE_TABLE2.csv
│       │   ├── CUSTOM_TABLE.csv
│       │   └── ...
│       └── post_test/
│           ├── UPDATE_TABLE1.csv
│           ├── UPDATE_TABLE2.csv
│           └── ...
```

Where:
- `SCRIPT_NAME`: The name of the script being tested (e.g., "EXAMPLE_ETL_SCRIPT")
- `test_name`: The specific test name (e.g., "test_basic", "test_advanced")
- `pre_test/`: Contains filtered reference tables and custom table data
- `post_test/`: Contains all update tables after ETL job execution

## Filter Resolution

The Test Builder uses a sophisticated filter resolution system to identify specific test data records:

### Filter Chain Process

1. **Filter Resolution Steps**: Each test defines a chain of steps to resolve filter values
2. **SQL-based Resolution**: Direct SQL queries can be used to find test data
3. **Table-based Resolution**: Sequential table lookups using filter expressions
4. **Filter Mapping**: Field mapping to standardize column names across tables

### Example Filter Resolution

```json
"filter_resolution": [
    {
        "sql": "SELECT DISTINCT B.RECORD_KEY, A.RECORD_ID, A.TRANSACTION_ID FROM SCHEMA1.VIEW_NAME as A...",
        "columns": ["TRANSACTION_ID", "RECORD_ID", "RECORD_KEY"]
    }
]
```

### Filter Expression Support

- **Placeholder Substitution**: Use `{RECORD_ID}` syntax in filter expressions
- **Dynamic Resolution**: Values are resolved from previous steps in the chain
- **Column Validation**: Only applicable columns are used for filtering

## Addon System

The TestBuilder includes a simple addon system that allows you to extend functionality without modifying the core TestBuilder code.

### Overview

The addon system consists of a single file: `TestBuilderAddon.py` that contains the `run_addon` method. This method is called by `DumpPreTestData.py` to execute custom table creation logic.

### How to Add Custom Logic

1. **Open `TestBuilderAddon.py`**
2. **Modify the `run_addon` method** to add your custom logic
3. **Add new helper methods** as needed for your specific requirements

### How to Use the Addon System

The `run_addon` method is called automatically by `DumpPreTestData.py` during the pre-test data generation phase. You can modify this method to add your custom logic.

#### Method Signature

```python
@staticmethod
def run_addon(test_builder, script_name: str, test_name: str) -> bool:
    """
    Run addon logic for the given script and test.
    
    Args:
        test_builder: TestBuilder instance with access to spark, output_volume, and config methods
        script_name: Name of the script being processed
        test_name: Name of the test being run
        
    Returns:
        bool: True if successful, False otherwise
    """
```

#### Available Resources

Within the `run_addon` method, you have access to:

- `test_builder.spark`: PySpark SparkSession for data processing
- `test_builder.output_volume`: Base output volume path
- `test_builder.get_test_config(script_name, test_name)`: Get script configuration from config.json
- `test_builder._ensure_directory_exists(path)`: Helper to create directories

#### Example Usage

```python
@staticmethod
def run_addon(test_builder, script_name: str, test_name: str) -> bool:
    """Run addon logic for the given script and test."""
    try:
        print(f"🔄 Running addon for script: {script_name}, test: {test_name}")
        
        # Get script configuration
        script_config = test_builder.get_test_config(script_name, test_name)
        if not script_config:
            return False
        
        # Add your custom logic here
        # Example: Create custom tables, perform data transformations, etc.
        
        return True
        
    except Exception as e:
        print(f"❌ Error running addon: {e}")
        return False
```

### Configuration for Custom Tables

Add your configuration to `config.json`:

```json
{
    "enabled": true,
    "test_name": "test_basic",
    "script": "MY_SCRIPT",
    "custom_config": {
        "PROCESS_NAME": "MY_PROCESS",
        "SOURCE_SYSTEM": "MY_SYSTEM",
        "PROCESSING_STATUS": "ACTIVE"
    },
    "my_custom_config": {
        "value1": "test_value1",
        "value2": "test_value2"
    }
}
```

### Addon Benefits

- **Simple**: Just one file to modify
- **Clean**: No complex interfaces or registries
- **Easy to understand**: Clear method structure
- **Flexible**: Add any custom logic you need
- **Isolated**: Changes don't affect core TestBuilder code

### Current Functionality

The addon system provides a framework for creating custom tables and business logic for ETL scripts. You can easily add more functionality or modify existing logic by editing `TestBuilderAddon.py`.

## Error Handling

- The Test Builder continues processing other tests even if one fails
- Individual table dump failures are logged but don't stop the process
- Filter resolution failures will skip the test but continue with others
- ETL job execution failures will stop the entire process
- All errors are logged with descriptive messages and emoji indicators

## Dependencies

- **PySpark**: For data processing and SQL execution
- **Databricks Runtime**: For notebook execution and dbutils
- **Test Framework**: Integration with `DataPrep` for environment setup
- **Access Requirements**:
  - Target catalog (source data)
  - Unit test catalog (test execution)
  - Output volume (test data storage)
  - Definitions volume (table/view definitions)
- **Configuration Files**: `config.json` and `script_metadata.json`

## Key Features

- **Intelligent Filter Resolution**: Automatically finds relevant test data using configurable filter chains
- **Unit Test Environment**: Creates isolated test environments using the test-framework
- **Addon System**: Extensible addon system for custom tables and business logic
- **Flexible Configuration**: Supports multiple test scenarios per script
- **Comprehensive Logging**: Detailed progress tracking with visual indicators
- **Error Resilience**: Continues processing even when individual components fail

## Notes

- **Data Filtering**: Uses sophisticated filter chains to extract only relevant test data
- **Timezone Consistency**: All operations use "America/Phoenix" timezone
- **CSV Formatting**: All output files include proper headers and null value handling
- **Directory Structure**: Follows the same pattern as existing `test_data` folder
- **Test Isolation**: Each test runs in its own isolated environment
