# LakeFlow ETL Unit Test Framework

A comprehensive testing framework, test builder, and utilities to demonstrate automated ETL testing capabilities for Databricks environments. This framework provides end-to-end testing solutions for ETL jobs with automated test data generation, execution, and validation.

## 🎯 Overview

The LakeFlow ETL Unit Test Framework is designed to streamline and automate the testing of ETL processes in Databricks. It consists of three main components that work together to provide comprehensive testing capabilities:

1. **Test Framework** - Core testing infrastructure for ETL job validation
2. **Test Builder** - Automated test data generation system  
3. **Utilities** - Utility scripts and tools for test data management and configuration

The framework integrates with the [lineage-analyzer](https://github.com/harrydu-db/lineage-analyzer) project to automatically generate test metadata from SQL lineage analysis, enabling intelligent test data generation and comprehensive ETL validation.

## 🏗️ Architecture

```
lakeflow-etl-unittest-framework/
├── tests/
│   ├── test-framework/          # Core testing infrastructure
│   ├── test-builder/            # Automated test data generation
│   └── utils/                   # Utility scripts and tools
├── test_data/                   # Test cases and definitions
├── old/                         # Example Teradata SQL code and lineage
├── resources/                   # Databricks job configurations
└── samples/                     # Sample SQL files
```

## 🚀 Key Features

### Test Framework
- **Automated Test Execution**: Runs ETL jobs and validates results
- **Data Preparation**: Creates tables and views from test data
- **Data Validation**: Compares expected results with actual table content
- **Databricks Integration**: Works with Databricks notebooks and clusters
- **Parallel Execution**: Supports both sequential and parallel test execution
- **Comprehensive Reporting**: Detailed validation reports and statistics

### Test Builder
- **Intelligent Filter Resolution**: Automatically finds relevant test data using configurable filter chains
- **Unit Test Environment**: Creates isolated test environments for ETL job execution
- **Automated Test Data Generation**: Creates realistic test data using sophisticated filter resolution
- **Addon System**: Extensible system for custom tables and business logic
- **Flexible Configuration**: Supports multiple test scenarios per script

### Sample Code
- **Teradata SQL Examples**: Complete examples of Teradata SQL code with DDL
- **Lineage Analysis**: Pre-generated lineage JSON files using the lineage-analyzer
- **Test Data**: Sample test cases demonstrating framework capabilities

### Utilities
- **Test Data Management**: Upload and download test data to/from Databricks volumes
- **Metadata Generation**: Generate script metadata from lineage analysis
- **Configuration Management**: Handle YAML-based configuration files
- **Data Consolidation**: Consolidate and process script metadata

## 📋 Prerequisites

- **Python 3.8+**: Required for local development
- **Databricks Workspace**: Access to a Databricks workspace with a running cluster
- **Databricks CLI**: Install and configure the Databricks CLI
- **Dependencies**: Install required packages from `requirements-dev.txt`

## 🛠️ Installation

1. **Clone the repository**:
   ```bash
   git clone <repository-url>
   cd lakeflow-etl-unittest-framework
   ```

2. **Install dependencies**:
   ```bash
   pip install -r requirements-dev.txt
   ```

3. **Configure Databricks CLI**:
   ```bash
   databricks configure --profile your-profile-name
   ```

4. **Update configuration**:
   - Update `tests/config.yml` with your Databricks profile and volume path
   - Configure `databricks.yml` with your workspace settings

## 🚀 Quick Start

### 1. Deploy the Framework

Deploy all components using Databricks Asset Bundle:

```bash
databricks bundle deploy -t dev -p your_profile
```

This creates the following jobs in your Databricks workspace:
- **test-builder job**: Generates test data using the test-builder components
- **unittest job**: Executes ETL job tests using the test framework
- **unittest_parallel job**: Runs tests in parallel for better performance
- **unittest_debmo job**: Sample ETL job demonstrating framework capabilities

### 2. Upload Test Data

Upload test data and definitions to your Databricks volume:

```bash
python tests/utils/upload_test_data.py
```

### 3. Run Tests

Execute tests using the deployed jobs or run directly:

```bash
# Using the deployed unittest job
# (Execute via Databricks UI or CLI)

# Or run locally (if using databricks-connect)
python tests/test-framework/TestRunner.py
```

## 📚 Documentation

### Core Components

- **[Test Framework Documentation](tests/test-framework/README.md)** - Detailed documentation for the core test framework
- **[Test Builder Documentation](tests/test-builder/README.md)** - Detailed documentation for automated test data generation
- **[Test Suite Overview](tests/README.md)** - Complete overview of the testing capabilities
- **[Utilities Documentation](tests/utils/README.md)** - Documentation for utility scripts and tools

### Key Features

#### Test Framework
- **Sequential Execution**: `TestRunner.py` runs tests one by one using a shared catalog
- **Parallel Execution**: `TestRunnerParallel.py` runs multiple tests simultaneously with isolated catalogs
- **Data Preparation**: `DataPrep.py` creates tables and views from test data
- **Data Validation**: `TestValidation.py` compares expected vs actual results

#### Test Builder
- **Filter Resolution**: Intelligent system to identify relevant test data
- **Addon System**: Extensible framework for custom business logic
- **Configuration Management**: Flexible JSON-based configuration system

## 🔧 Configuration

### Test Metadata

The framework uses `script_metadata.json` files that contain dependency information for ETL jobs. These files are generated from lineage analysis:

1. **Lineage Analysis**: [lineage-analyzer](https://github.com/harrydu-db/lineage-analyzer/) analyzes ETL jobs and generates lineage JSON files
2. **Metadata Generation**: `build_test_metadata.py` processes lineage files to create comprehensive test metadata

### Test Data Structure

```
test_data/
├── table_definitions/           # JSON schema definitions for all tables
├── view_definitions/            # SQL definitions for all views
└── {script_name}/{test_name}/   # Individual test case data
    ├── pre_test/                # Input data (CSV files) before ETL execution
    └── post_test/               # Expected output data (CSV files) after ETL execution
```

## 📊 Example Usage

### Sample Teradata SQL Code

The `old/` folder contains example Teradata SQL code demonstrating the framework:

- **DDL Files**: Table creation scripts in `old/ddl/`
- **Source SQL**: ETL job examples in `old/source/`
- **Lineage Data**: Pre-generated lineage JSON files in `old/lineage/`

These examples show how the [lineage-analyzer](https://github.com/harrydu-db/lineage-analyzer) processes SQL code to generate lineage information that feeds into the test framework.

### Test Workflow

1. **Generate Test Data**: Use the test-builder to automatically generate realistic test data
2. **Run Tests**: Execute ETL jobs using the test framework components
3. **Validate Results**: Compare generated post-test data with expected results

## 🔄 Integration with Lineage Analyzer

The framework integrates seamlessly with the [lineage-analyzer](https://github.com/harrydu-db/lineage-analyzer) project:

1. **Lineage Analysis**: Analyzes SQL code to extract table dependencies and relationships
2. **Metadata Generation**: Converts lineage data into test metadata
3. **Test Data Generation**: Uses metadata to generate intelligent test data
4. **Validation**: Validates ETL job results against expected outcomes

## 🛡️ Error Handling

- **Graceful Degradation**: Continues execution even when individual components fail
- **Detailed Logging**: Comprehensive error messages and debugging information
- **Validation Reports**: Saves detailed validation results to JSON files
- **Progress Tracking**: Real-time progress and status updates

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🙏 Acknowledgments

- Built with [lineage-analyzer](https://github.com/harrydu-db/lineage-analyzer) for SQL lineage analysis
- Databricks Asset Bundle for deployment and configuration
- PySpark for data processing and validation
- Made with help from Cursor AI

## 📞 Support

For questions or issues with the framework, please refer to the individual component documentation or contact the development team.

---

**Note**: This framework is designed to work with Databricks environments and requires proper configuration of Databricks CLI and workspace access. See the individual component documentation for detailed setup instructions.
