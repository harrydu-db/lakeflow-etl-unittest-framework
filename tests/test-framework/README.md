# Databricks ETL Test Framework

This directory contains a comprehensive test framework for validating ETL jobs using Databricks notebooks and automated data validation. The framework provides a standardized approach to testing ETL processes with automated data preparation, execution, and validation.

## Overview

The test framework provides comprehensive automated testing capabilities for ETL jobs through a three-phase workflow:

1. **Data Preparation** - Creates tables and views from test data
2. **ETL Job Execution** - Runs the ETL job being tested  
3. **Data Validation** - Compares expected results with actual table content

## Prerequisites

1. **Python Environment**: Ensure you have Python 3.8+ installed
2. **Databricks Workspace**: Access to a Databricks workspace with a running cluster
3. **Databricks CLI**: Install and configure the Databricks CLI
4. **Dependencies**: Install the required packages from `requirements-dev.txt`

## Test Process

The test framework follows a standardized three-step process for each test:

### 1. Prepare Test Environment (DataPrep)
- **Purpose**: Set up the test environment with pre-test data
- **Process**: 
  - Creates required catalogs and schemas
  - Loads CSV files from `pre_test/` folder into Delta tables
  - Creates views from SQL definitions in dependency order
  - Validates data structure and schema compliance
- **Input**: Pre-test CSV files and table/view definitions
- **Output**: Fully prepared test environment ready for ETL execution

### 2. Execute ETL Job
- **Purpose**: Run the actual ETL job being tested
- **Process**:
  - Executes the ETL job script from the configured source directory
  - Uses the prepared test environment as input
  - Processes data according to business logic
  - Updates target tables with results
- **Input**: Pre-test data from prepared environment
- **Output**: ETL job results in target tables

### 3. Validate Test Results (TestValidation)
- **Purpose**: Verify that ETL job produced expected results
- **Process**:
  - Compares actual table content with expected post-test data
  - Validates row counts, column structure, and data content
  - Generates detailed validation reports
  - Reports any discrepancies or validation failures
- **Input**: Post-test CSV files and actual table content
- **Output**: Validation results and test pass/fail status

This three-step process ensures comprehensive testing by validating both the input preparation and output verification phases of ETL job execution.

## Core Components

### Test Execution Orchestrators

#### `TestRunner.py` (Sequential Runner)
The main test orchestrator that executes tests sequentially using a single shared catalog. Features:
- **Sequential Test Execution**: Runs tests one by one in order
- **Single Catalog Usage**: All tests share the same catalog (`unittest_catalog`)
- **Progress Tracking**: Shows real-time progress with test counts and status
- **Summary Reporting**: Provides detailed pass/fail statistics and success rates
- **Error Handling**: Continues execution even if individual tests fail
- **Resource Efficient**: Lower resource usage but longer execution time

#### `TestRunnerParallel.py` (Parallel Runner)
Advanced parallel test executor that creates isolated catalogs for each test. Features:
- **Parallel Execution**: Runs multiple tests simultaneously using ThreadPoolExecutor
- **Isolated Catalogs**: Each test gets its own unique catalog (e.g., `unittest_catalog_script_name_test_name`)
- **Conflict Prevention**: Eliminates test interference by using separate catalogs
- **Configurable Workers**: Adjustable parallelism with `max_workers` parameter
- **Comprehensive Results**: Detailed reporting of parallel execution results
- **Higher Performance**: Faster execution but requires more resources

### Data Preparation

#### `DataPrep.py`
Databricks notebook that prepares the test environment by creating tables and views. Features:
- **Catalog Management**: Automatically creates and configures test catalogs
- **Schema Setup**: Creates all required schemas as configured
- **Table Creation**: Loads CSV data into Delta tables with proper schema handling
- **View Creation**: Creates views from SQL definitions in dependency order

#### `TableAndViewCreator.py`
Core class for creating tables and views with advanced features:
- **Schema Validation**: Validates CSV structure before processing
- **Type Mapping**: Converts JSON schema definitions to Spark data types
- **CSV Processing**: Handles both single files and partitioned data (part-*.csv)
- **Error Recovery**: Graceful handling of missing data and schema issues
- **Catalog Preparation**: Automated catalog and schema creation

### Data Validation

#### `TestValidation.py`
Databricks notebook that validates test results by comparing expected vs actual data. Features:
- **Automated Validation**: Compares CSV data with actual table content
- **Comprehensive Checks**: Validates row counts, column structure, and data content
- **Report Generation**: Saves detailed validation reports to JSON files
- **Error Reporting**: Provides detailed error messages and debugging information

#### `DataValidator.py`
Advanced validation class with sophisticated comparison capabilities:
- **CSV Structure Validation**: Validates CSV files for proper formatting and structure
- **Data Comparison**: Uses Spark operations for efficient data comparison
- **Null Value Handling**: Normalizes null values for accurate comparison
- **Timestamp Handling**: Skips problematic timestamp columns during comparison
- **Detailed Reporting**: Generates comprehensive validation reports with field-level differences

### Metadata Management

#### `MetadataLoader.py`
Utility for loading test and script metadata from JSON files:
- **Test Metadata**: Loads test configuration and enabled test lists
- **Script Metadata**: Loads script dependencies and table/view definitions
- **Configuration Access**: Provides easy access to test framework configuration

## Test Workflow

### Sequential Execution (TestRunner.py)

```python
# Uses single shared catalog: unittest_catalog
# For each enabled test (one at a time):
1. DataPrep → Creates tables and views in shared catalog
2. ETL Job → Executes the ETL job in shared catalog
3. TestValidation → Validates results against expected data
4. Repeat for next test (reuses same catalog)
```

### Parallel Execution (TestRunnerParallel.py)

```python
# Creates unique catalog per test: unittest_catalog_{script_name}_{test_name}
# For all enabled tests simultaneously:
1. Create isolated catalog for each test
2. Run DataPrep, ETL Job, and TestValidation in parallel
   (each test uses its own catalog)
3. Collect and report results from all parallel executions
```

## Configuration Files

### `test_metadata.json`
Defines which tests to run and their configuration:
```json
{
  "src_dir": "src/etl_jobs",
  "test_data_volume": "/Volumes/{target_catalog}/default/test_data",
  "schemas": ["schema1", "schema2", "schema3"],
  "enabled_tests": [
    {
      "script": "example_script",
      "folder": "category", 
      "tests": ["test_basic", "test_advanced"]
    }
  ]
}
```

### `script_metadata.json`
Contains script dependencies and table/view definitions:
```json
{
  "example_script": {
    "tables": ["SCHEMA1.TABLE1", "SCHEMA2.TABLE2"],
    "views": ["SCHEMA1.VIEW1"],
    "update_tables": ["SCHEMA1.TABLE1"],
    "reference_tables": ["SCHEMA2.TABLE2"]
  }
}
```

## Integration with Supporting Components

### Test Data (`test_data/`)
- **`table_definitions/`** - JSON schema definitions for all tables
- **`view_definitions/`** - SQL definitions for all views
- **`{script_name}/{test_name}/`** - Test data with pre_test and post_test CSV files

### Test Builder (`test-builder/`)
- **Automated Test Data Generation**: Creates realistic test data using intelligent filter resolution
- **Integration**: Works seamlessly with the test framework by providing pre-generated test data
- **Documentation**: See [test-builder/README.md](../test-builder/README.md) for detailed usage

### Utilities (`utils/`)
- **`upload_test_data.py`** - Uploads test data to Databricks volumes
- **`download_test_data.py`** - Downloads test data from Databricks volumes
- **`build_test_metadata.py`** - Generates script metadata from lineage information
- **`config.py`** - Configuration management using YAML files

## Running Tests

### Via Databricks Bundle (Recommended)

The unit test job is automatically deployed as part of the asset bundle deployment process:

```bash
# Deploy the bundle (includes unit test job deployment)
databricks bundle deploy -t dev -p your_profile
```

The unit test job is configured in `databricks.yml` and the corresponding job configuration file and becomes available in your Databricks workspace after bundle deployment.

### Direct Notebook Execution

1. **Sequential Runner** (`TestRunner.py`):
   - `unittest_catalog`: Single shared catalog name for all tests
   - All tests run in the same catalog sequentially

2. **Parallel Runner** (`TestRunnerParallel.py`):
   - `unittest_catalog`: Base catalog name (each test gets unique catalog)
   - `max_workers`: Number of parallel workers
   - Each test gets its own catalog: `{unittest_catalog}_{script_name}_{test_name}`

## Test Data Structure

Each test case follows this structure:

```
test_data/
├── {script_name}/
│   └── {test_name}/
│       ├── pre_test/          # Input data (CSV files)
│       └── post_test/         # Expected output data (CSV files)
```

## Advanced Features

### Schema Handling
- **Automatic Type Detection**: Converts JSON schema definitions to Spark data types
- **Collation Support**: Handles StringType with collation specifications
- **Decimal Precision**: Supports DecimalType with custom precision and scale
- **Nullable Fields**: Respects nullable constraints from schema definitions

### CSV Processing
- **Partitioned Data**: Supports both single CSV files and partitioned data (part-*.csv)
- **Structure Validation**: Validates CSV structure before processing
- **Error Recovery**: Handles missing files and malformed data gracefully
- **Multi-line Support**: Handles CSV files with multi-line fields

### Data Validation
- **Comprehensive Comparison**: Validates row counts, column structure, and data content
- **Null Value Normalization**: Handles various null value representations consistently
- **Timestamp Handling**: Skips problematic timestamp columns during comparison
- **Field-level Differences**: Provides detailed field-level comparison results

### Error Handling
- **Graceful Degradation**: Continues execution even when individual components fail
- **Detailed Logging**: Provides comprehensive error messages and debugging information
- **Validation Reports**: Saves detailed validation results to JSON files
- **Progress Tracking**: Shows real-time progress and status updates

