from src.utility.report_lib import write_output


def uniqueness_check(df, unique_cols):
    """Validate that specified columns have unique values."""
    duplicate_counts = {}
    for column in unique_cols:
        count_duplicates = df.groupBy(column).count().filter("count > 1").count()
        print("count_duplicates", column, count_duplicates)
        duplicate_counts[column] = count_duplicates  # {

    print("duplicate_counts", duplicate_counts)

    status = "PASS" if all(count == 0 for count in duplicate_counts.values()) else "FAIL"

    write_output(
        "Uniqueness Check",
        status,
        f"Duplicate counts per column: {duplicate_counts}"
    )
    return status
