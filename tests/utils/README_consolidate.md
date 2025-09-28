# Script Metadata Consolidator

This utility consolidates the `script_metadata.json` file into a single consolidated structure that provides a unified view of all tables, views, and their relationships across all scripts.

## Usage

```bash
python tests/utils/consolidate_script_metadata.py <input_file_path>
```

### Example

```bash
python tests/utils/consolidate_script_metadata.py script_metadata.json
```

This will generate `script_metadata_consolidated.json` in the same directory as the input file.

## Output Structure

The consolidated JSON file contains the following fields:

- **`tables`**: All unique tables from all scripts, sorted alphabetically
- **`views`**: All unique views from all scripts, maintaining topological order (dependencies first)
- **`reference_tables`**: All unique reference tables from all scripts, sorted alphabetically
- **`updated_tables`**: All unique updated tables from all scripts, sorted alphabetically
- **`reference_only_tables`**: Reference tables that are not updated tables (reference_tables - updated_tables), sorted alphabetically

## Key Features

1. **Deduplication**: Removes duplicate entries across all scripts
2. **Topological Ordering**: Views maintain their dependency order across scripts
3. **Alphabetical Sorting**: Tables, reference tables, updated tables, and reference-only tables are sorted alphabetically
4. **Dependency Analysis**: Builds a dependency graph to ensure views are ordered correctly

## Example Output

```json
{
  "tables": [
    "BIZT.BIZT_BATCHTRACK",
    "LOTMASTER_BASE_T.LOT",
    "TAS_CORE.CORE_MATERIAL"
  ],
  "views": [
    "BIZT.BIZT_BATCHTRACK_V",
    "LOTMASTER.LOT_SO_DTL",
    "REFERENCE.MATERIAL"
  ],
  "reference_tables": [
    "BIZT.BIZT_BATCHTRACK",
    "TAS_CORE.CORE_MATERIAL"
  ],
  "updated_tables": [
    "LOTMASTER_BASE_T.LOT"
  ],
  "reference_only_tables": [
    "BIZT.BIZT_BATCHTRACK",
    "TAS_CORE.CORE_MATERIAL"
  ]
}
```

## Dependencies

- Python 3.6+
- Standard library modules: `json`, `os`, `sys`, `collections`

## Notes

- The utility preserves the topological order of views by analyzing dependencies within each script
- Reference-only tables are calculated as the set difference between reference tables and updated tables
- All lists (except views) are sorted alphabetically for consistent ordering
