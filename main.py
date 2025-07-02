from pyspark.sql import SparkSession
import os
import yaml
import json
from pyspark.sql.types import StructType
# Create SparkSession
spark = SparkSession.builder \
      .master("local[1]") \
      .appName("SparkByExamples.com") \
      .getOrCreate()


df = spark.read.csv("C:\\Users\\Admin\\PycharmProjects\\eal_framework_april_2025\\input_files\\Contact_info.csv", header=True, sep=',', inferSchema=True)

df.show()

df.printSchema()

print(df.schema.json())
#
# with open("C:\\Users\\Admin\\PycharmProjects\\eal_framework_april_2025\\input_files\\customer_data\\customer_data_schema.json", 'r') as schema_file:
#       schema = StructType.fromJson(json.load(schema_file))
#
# print("Schema is", schema)


#
#
# print("df with schema")
# df_with_schema = spark.read.schema(schema).csv("C:\\Users\\Admin\\PycharmProjects\\eal_framework_april_2025\\input_files\\customer_data\\customer_data_04.csv", header=False, sep=',')
#
# df_with_schema.show()
#
# df_with_schema.printSchema()