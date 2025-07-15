from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('test').getOrCreate()

vendor1 = spark.read.csv(
    r"C:\Users\Admin\PycharmProjects\eal_framework_april_2025\input_files\customer_data\customer_data_01.csv",
    header=True, inferSchema=True)


vendor2 = spark.read.csv(
    r"C:\Users\Admin\PycharmProjects\eal_framework_april_2025\input_files\customer_data\customer_data_03.csv",
    header=True)

vendor3 =  spark.read.format("jdbc"). \
            option("url", creds['url']). \
            option("user", creds['user']). \
            option("password", creds['password']). \
            option("dbtable", config['table']). \
            option("driver", creds['driver']).load()

final_df = vendor1.union(vendor2).union(vendor3)

final_df.write.mode('overwrite').csv(r"C:\Users\Admin\PycharmProjects\eal_framework_april_2025\input_file\orders")

