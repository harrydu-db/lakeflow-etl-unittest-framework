from pyspark.sql.functions import col
from pyspark.sql.types import StringType

def find_value_in_table(table_name, value_to_find="2.5E1"):
    """
    For each string column in the table, count how many times the value appears.
    """
    try:
        df = spark.table(table_name)
        string_columns = [field.name for field in df.schema.fields if isinstance(field.dataType, StringType)]
        found_columns = []
        for col_name in string_columns:
            count = df.filter(col(col_name) == value_to_find).count()
            if count > 0:
                found_columns.append({
                    "table_name": table_name,
                    "column_name": col_name,
                    "value_found": value_to_find,
                    "count": count,
                    "total_rows": df.count()
                })
        return found_columns
    except Exception as e:
        print(f"Error processing table {table_name}: {str(e)}")
        return None

def find_value_in_all_tables(table_list, value_to_find="2.5E1"):
    all_results = []
    for table_name in table_list:
        print(f"Searching table: {table_name}")
        results = find_value_in_table(table_name, value_to_find)
        if results:
            all_results.extend(results)
        else:
            print(f"No results found in table {table_name}")
    return all_results

def search_specific_tables():
    tables_to_search = [
        "1dp_migration_lotmaster_delta_poc.BIZT.PROMIS_BIZT_LOTTRACKOUT_V",
        "1dp_migration_lotmaster_delta_poc.BIZT.BIZT_RESP_MSG_LM_V"
    ]
    print("Starting search for value '2.5E1' in tables...")
    results = find_value_in_all_tables(tables_to_search, "2.5E1")
    if results:
        print("Found the following results:")
        for result in results:
            print(f"Table: {result['table_name']}")
            print(f"Column: {result['column_name']}")
            print(f"Value: {result['value_found']}")
            print(f"Count: {result['count']}")
            print(f"Total Rows: {result['total_rows']}")
            print("-" * 50)
    else:
        print("No occurrences of '2.5E1' found in any table")
    return results