from pyspark.sql import SparkSession
import os
import yaml
import json
from pyspark.sql.types import StructType
# Create SparkSession

path = os.path.abspath(__file__)
#
print(path)

path_dir = os.path.dirname(os.path.abspath(__file__))
print(path_dir)

path_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
print(path_dir)

path_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
print(path_dir)
# #
# #
# jar_path = "C:\\Users\\Admin\\PycharmProjects\\eal_framework_april_2025\\jars\\mssql-jdbc-12.2.0.jre8.jar"
#
# spark = SparkSession.builder \
#       .master("local[1]") \
#       .appName("Automation") \
#       .config("spark.jars", jar_path) \
#       .config("spark.driver.extraClassPath", jar_path) \
#       .config("spark.executor.extraClassPath", jar_path) \
#       .getOrCreate()
#
df = spark.read.format("jdbc"). \
      option("url", "jdbc:sqlserver://decautoserver.database.windows.net:1433;database=decauto"). \
      option("user", 'decadmin'). \
      option("password", "Dharmavaram1@"). \
      option("query", "SELECT *  FROM [dbo].[customers_raw]"). \
      option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver").load()
#
# df.show()
#
