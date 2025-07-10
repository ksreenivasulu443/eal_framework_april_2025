from pyspark.sql.functions import lit, col, when
from src.utility.report_lib import write_output

# from pyspark.sql import SparkSession
#
# spark = SparkSession.builder.appName('test').getOrCreate()
#
# source = spark.read.csv(
#     r"C:\Users\Admin\PycharmProjects\eal_framework_april_2025\input_files\customer_data\customer_data_01.csv",
#     header=True)
#
# target = spark.read.csv(
#     r"C:\Users\Admin\PycharmProjects\eal_framework_april_2025\input_files\customer_data\customer_data_03.csv",
#     header=True)
#
# key_columns = ['customer_id']


def data_compare(source, target, key_column, num_records=5):
    smt = source.exceptAll(target).withColumn("datafrom", lit("source"))
    tms = target.exceptAll(source).withColumn("datafrom", lit("target"))
    failed = smt.union(tms)
    failed_count = failed.count()
    if failed_count > 0:
        failed_records = failed.limit(num_records).collect()  # Get the first 5 failing rows
        print("failed_records", failed_records)
        failed_preview = [row.asDict() for row in failed_records]
        print("failed_preview", failed_preview)
        write_output(
            "data compare Check",
            "FAIL",
            f"Data mismatch data: {failed_preview}"
        )
    else:
        write_output(
            "data compare Check",
            "PASS",
            f"No mismatches found"
        )

    if failed_count > 0:

        columnList = source.columns  # ['id','first_name','last_name','salary']
        print("columnList", columnList)
        print("keycolumns", key_column)  #['id']
        for column in columnList:  #column ='last_name'
            print(column.lower())
            if column not in key_column:  # 'last_name' not in ['id']
                key_column.append(column)  # ['id','last_name']
                temp_source = source.select(key_column).withColumnRenamed(column, "source_" + column)

                temp_target = target.select(key_column).withColumnRenamed(column, "target_" + column)
                key_column.remove(column)  # ['id']
                temp_join = temp_source.join(temp_target, key_column, how='full_outer')
                (temp_join.withColumn("comparison", when(col('source_' + column) == col("target_" + column),
                                                         "True").otherwise("False")).
                 filter("comparison == False ").show())

        status = 'FAIL'

        return status
    else:
        status = 'PASS'
        return status


# data_compare(source=source, target=target, key_column=key_columns, num_records=5)
