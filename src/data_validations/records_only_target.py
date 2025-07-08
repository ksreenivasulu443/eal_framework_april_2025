from src.utility.report_lib import write_output


# from pyspark.sql import SparkSession
#
# spark = SparkSession.builder.appName('test').getOrCreate()
#
# source = spark.read.csv(r"C:\Users\Admin\PycharmProjects\eal_framework_april_2025\input_files\customer_data\customer_data_01.csv", header=True)
#
# target = spark.read.csv(r"C:\Users\Admin\PycharmProjects\eal_framework_april_2025\input_files\customer_data\customer_data_02.csv", header=True)
#
# key_columns = ['customer_id']
def records_only_in_target(source_df, target_df, key_columns):
    """Validate records present only in the target."""
    only_in_target = target_df.select(key_columns).exceptAll(source_df.select(key_columns))

    count_only_in_target = only_in_target.count()
    if count_only_in_target > 0:
        failed_records = only_in_target.limit(5).collect()  # Get the first 5 failing rows
        failed_preview = [row.asDict() for row in failed_records]  # Convert rows to a dictionary for display
        status = "FAIL"
        write_output(
            validation_type="Records Only in Target",
            status=status,
            details=f"Count: {count_only_in_target}, Sample Failed Records: {failed_preview}"
        )

    else:
        status = "PASS"
        write_output(
            validation_type="Records Only in Target",
            status=status,
            details="No extra records found in target.")
    return status

# records_only_in_target(source_df=source, target_df=target, key_columns=key_columns)

