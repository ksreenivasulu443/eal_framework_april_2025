from pyspark.sql import SparkSession
import os
import yaml
# Create SparkSession
spark = SparkSession.builder \
      .master("local[1]") \
      .appName("SparkByExamples.com") \
      .getOrCreate()
dataList = [("Java", 20000), ("Python", 100000), ("Scala", 3000)]
df=spark.createDataFrame(dataList, schema=['Language','fee'])

df.show()


def test_config(request):
    #config_path = request.node.fspath.dirname + '/config.yml'
    dir_path = request.node.fspath.dirname

    print("request.node", request.node)
    print("request.node.fspath", request.node.fspath)
    print("request.node.fspath.dirname", request.node.fspath.dirname)
    config_path = os.path.join(dir_path, "config.yml")
    print("config path", config_path)
    with open(config_path, 'r') as f:
        config_data = yaml.safe_load(f)
    print(config_data)
    return config_data