from pyspark.sql.functions import col, trim
from src.utility.report_lib import write_output
from pyspark.sql.functions import upper
from pyspark.sql import SparkSession


def null_value_check(df, null_cols):
    """Validate that specified columns have no null or empty values."""
    failures = []

    for column in null_cols:
        print("column", column)
        failing_rows = df.filter(
            (col(column).isNull()) | (trim(col(column)) == "") | (upper(col(column)) == "NULL") | (upper(col(column)) == "na"))
        null_count = failing_rows.count()
        print("null_count", null_count)
        if null_count > 0:
            failed_records = failing_rows.limit(5).collect()  # Get the first 5 failing rows
            print("failed_records", failed_records)
            failed_preview = [row.asDict() for row in failed_records]  # Convert rows to a dictionary for display
            print("failed preview", failed_preview)
            failures.append({
                "column": column,
                "null_count": null_count,
                "sample_failed_records": failed_preview
            })
            print("failures", failures)

    if len(failures)>=1:
        status = "FAIL"
        write_output("Null Value Check", status, f"Failures: {failures}")
        return status
    else:
        status = "PASS"
        write_output("Null Value Check", status, "No null values found.")
        return status
