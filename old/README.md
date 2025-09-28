# Samples Directory

This directory contains sample files demonstrating how to use the SQL Converter Agents suite. The samples show the complete workflow from source SQL files through conversion, optimization, validation, and analysis.

## Directory Structure

```
old_code/
   ├── README.md               # This file
   ├── source/                 # Sample SQL files for conversion
   │   ├── sample.sh           # Sample shell script with SQL
   │   ├── sample.sql          # Sample SQL file
   │   └── DEMO.PRODUCT_V.sql  # Additional sample SQL
   ├── ddl/                    # DDL files for table definitions
   │   └── DEMO.*.sql          # Table creation scripts
   └── lineage/                # Lineage data for agents
       ├── all_lineage.txt     # Summary of lineage data
       ├── processing_summary.yaml
       └── *.json              # Individual lineage files

```

## Usage Workflow

This samples directory demonstrates the complete SQL Converter Agents workflow using sample data.

### 1. Source Files (`source/` folder)

The `source/` folder contains sample SQL files that will be processed by the SQL Converter Agents:

- `sample.sql` - Main sample SQL file for conversion
- `sample.sh` - Sample shell script with embedded SQL
- `DEMO.PRODUCT_V.sql` - Additional sample SQL file

### 2. Supporting Data (`data_types/`, `ddl/`, `lineage/` folders)

These folders contain supporting data that the agents can use:

- **`data_types/`** - JSON files with table data type definitions for accurate conversion
- **`ddl/`** - DDL files for table structure information
- **`lineage/`** - Lineage data for understanding table relationships

### 3. Running the SQL Converter Agents

Use the sample configuration to test all agents:

```bash
# 1. Convert SQL (using config.json with samples configuration)
python src/converter/sql_converter.py

# 2. Optimize converted SQL
python src/converter/sql_optimizer_agent.py

# 3. Validate conversion
python src/converter/sql_validator_agent.py

# 4. Validate type casting
python src/converter/type_casting_validator_agent.py

# 5. Evaluate architecture
python src/converter/sql_architecture_evaluator_agent.py

# 6. Extract data expectations
python src/converter/data_expectation_agent.py
```

### 4. Output Files (`output/` folder)

The agents generate various output files:

- `sample.sql` - Converted Databricks SQL
- `sample_optimized.sql` - Performance-optimized SQL
- `sample_expectations.json` - Extracted data expectations
- `sample_architecture_evaluation.json` - Architecture assessment
- `*.log` files - Detailed processing logs

### 5. Configuration

The samples use the default `config.json` which is configured for the samples directory:

```json
{
  "data_types_folder": "./samples/input/data_types",
  "lineage_folder": "./samples/input/lineage",
  "input_folder": "samples/input/source",
  "output_folder": "samples/output",
  "script_names": ["sample"]
}
```

For more detailed information about the agents and their configuration options, refer to the main project documentation.
