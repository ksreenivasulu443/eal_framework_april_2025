from pyspark.sql import SparkSession
import pytest
import yaml


@pytest.fixture(scope="session")
def spark_session():
    spark = SparkSession.builder.master("local[1]").appName("Automation").getOrCreate()
    return spark

@pytest.fixture
def read_config(request):
    path = request.node.fspath.dirname
    config_path = path + '\\config.yml'
    with open(config_path, 'r') as f:
        config_data = yaml.safe_load(f)
    return config_data



