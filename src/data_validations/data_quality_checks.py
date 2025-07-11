from pyspark.sql.functions import col, regexp_extract, udf
import datetime
from pyspark.sql.types import BooleanType
from src.utility.report_lib import write_output  # Make sure this path is correct


def pattern_check(target, column, pattern):
    df = target.withColumn("is_valid", regexp_extract(col(column), pattern, 0) != "")
    df.show()

    target_count = target.count()
    failed_df = df.filter('is_valid = False')
    failed_df.show()

    failed_count = failed_df.count()
    status = 'FAIL' if failed_count > 0 else 'PASS'

    if failed_count > 0:
        failed_preview = failed_df.limit(5).collect()
        failed_preview_dict = [row.asDict() for row in failed_preview]
        write_output(
            "Name Check",
            status,
            f"Total Records: {target_count}, Failed Count: {failed_count}, Sample Failed Records: {failed_preview_dict}"
        )
    else:
        write_output("Name Check", status, "All names start with an alphabet.")

    return status


def check_range(target, column, min_val, max_val):
    invalid_df = target.filter((col(column) < min_val) | (col(column) > max_val))
    invalid_count = invalid_df.count()
    total_count = target.count()
    status = 'FAIL' if invalid_count > 0 else 'PASS'

    if invalid_count > 0:
        failed_preview = invalid_df.limit(5).collect()
        failed_preview_dict = [row.asDict() for row in failed_preview]
        write_output(
            "Range Check",
            status,
            f"Column: {column}, Min: {min_val}, Max: {max_val}, Failed Count: {invalid_count}, Sample Failed Records: {failed_preview_dict}"
        )
    else:
        write_output("Range Check", status, f"All values in '{column}' are within the range {min_val} to {max_val}.")

    return status


def date_check(target, column):
    def is_valid_date_format(date_str: str) -> bool:
        try:
            datetime.datetime.strptime(date_str, "%d-%m-%Y")
            return True
        except Exception:
            return False

    date_format_udf = udf(is_valid_date_format, BooleanType())

    df_with_validation = target.withColumn("is_valid_format", date_format_udf(col(column)))
    failed_df = df_with_validation.filter("is_valid_format = False")

    failed_count = failed_df.count()
    status = 'FAIL' if failed_count > 0 else 'PASS'

    if failed_count > 0:
        failed_preview = failed_df.limit(5).collect()
        failed_preview_dict = [row.asDict() for row in failed_preview]
        write_output(
            "Date Format Check",
            status,
            f"Column: {column}, Expected Format: dd-mm-yyyy, Failed Count: {failed_count}, Sample Failed Records: {failed_preview_dict}"
        )
    else:
        write_output("Date Format Check", status, f"All dates in column '{column}' match format dd-mm-yyyy.")

    return status


def value_set_check(df, column, allowed_values):
    invalid_df = df.filter(~col(column).isin(allowed_values))
    failed_count = invalid_df.count()
    status = 'FAIL' if failed_count > 0 else 'PASS'

    if failed_count > 0:
        failed_preview = invalid_df.limit(5).collect()
        failed_preview_dict = [row.asDict() for row in failed_preview]
        write_output("Value Set Check", status,
                     f"Invalid values in column '{column}'. Allowed: {allowed_values}, Failed Count: {failed_count}, Sample: {failed_preview_dict}")
    else:
        write_output("Value Set Check", status, f"All values in '{column}' are within allowed set.")

    return status


from pyspark.sql.functions import length


def length_check(df, column, min_len=None, max_len=None):
    conditions = []
    if min_len is not None:
        conditions.append(length(col(column)) < min_len)
    if max_len is not None:
        conditions.append(length(col(column)) > max_len)

    invalid_df = df.filter(conditions[0] if len(conditions) == 1 else conditions[0] | conditions[1])
    failed_count = invalid_df.count()
    status = 'FAIL' if failed_count > 0 else 'PASS'

    if failed_count > 0:
        failed_preview = invalid_df.limit(5).collect()
        failed_preview_dict = [row.asDict() for row in failed_preview]
        write_output("Length Check", status,
                     f"Column: {column}, Failed Count: {failed_count}, Sample: {failed_preview_dict}")
    else:
        write_output("Length Check", status,
                     f"All values in column '{column}' have lengths within range {min_len}-{max_len}.")

    return status


def non_negative_check(df, column):
    invalid_df = df.filter(col(column) < 0)
    failed_count = invalid_df.count()
    status = "FAIL" if failed_count > 0 else "PASS"

    if status == "FAIL":
        failed_preview = invalid_df.limit(5).collect()
        failed_preview_dict = [row.asDict() for row in failed_preview]
        write_output("Non-Negative Value Check", status,
                     f"Negative values found in '{column}'. Failed Count: {failed_count}, Sample: {failed_preview_dict}")
    else:
        write_output("Non-Negative Value Check", status, f"All values in '{column}' are non-negative.")

    return status


def record_count_check(df, min_count):
    record_count = df.count()
    status = "PASS" if record_count >= min_count else "FAIL"

    if status == "FAIL":
        write_output("Record Count Check", status,
                     f"Record count is below threshold. Expected ≥ {min_count}, Found: {record_count}")
    else:
        write_output("Record Count Check", status,
                     f"Record count is sufficient: {record_count} (threshold ≥ {min_count})")

    return status
