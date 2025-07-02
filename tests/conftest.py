from pyspark.sql import SparkSession
import pytest
import yaml
import os
from src.utility.general_utility import flatten
import json
from pyspark.sql.types import StructType


@pytest.fixture(scope="session")
def spark_session():
    spark = SparkSession.builder.master("local[1]").appName("Automation").getOrCreate()
    return spark


@pytest.fixture(scope='module')
def read_config(request):
    dir_path = request.node.fspath.dirname
    config_path = os.path.join(dir_path, "config.yml")
    with open(config_path, 'r') as f:
        config_data = yaml.safe_load(f)
    print("==" * 100)
    print("Config data is ", end='\n')
    print(config_data)
    print("==" * 100)
    return config_data

def read_query(dir_path):
    sql_query_path = os.path.join(dir_path, "transformation.sql")
    with open(sql_query_path, "r") as file:
        sql_query = file.read()
    return sql_query



def read_db(spark, config, dir_path):
    creds = load_credentials()
    cred_lookup = config['cred_lookup']
    creds = creds[cred_lookup]
    print("creds", creds)
    if config['transformation'][0].lower() == 'y' and config['transformation'][1].lower() == 'sql':
        sql_query= read_query(dir_path)
        print("sql_query", sql_query)
        df = spark.read.format("jdbc"). \
            option("url", creds['url']). \
            option("user", creds['user']). \
            option("password", creds['password']). \
            option("query", sql_query). \
            option("driver", creds['driver']).load()

    else:
        df = spark.read.format("jdbc"). \
            option("url", creds['url']). \
            option("user", creds['user']). \
            option("password", creds['password']). \
            option("dbtable", config['table']). \
            option("driver", creds['driver']).load()
    return df



def read_schema(dir_path):
    schema_path = os.path.join(dir_path, "schema.json")
    with open(schema_path, 'r') as schema_file:
        schema = StructType.fromJson(json.load(schema_file))
    return schema


def read_file(spark, config, dir_path):
    filetype = config['type'].lower()
    path = config['path']
    df = None
    if filetype == 'csv':
        if config['schema'].lower() == 'y':
            schema = read_schema(dir_path)
            df = spark.read.schema(schema).csv(config['path'], header=config['options']['header'],
                                               sep=config['options']['delimiter'])
        else:
            df = spark.read.csv(config['path'], header=config['options']['header'], inferSchema=config['options']['inferSchema'])
    elif filetype == 'json':
        df = spark.read.json(path, multiLine=config['options']['multiline'])
        df = flatten(df)
    elif filetype == 'parquet':
        df = spark.read.parquet(path)
    elif filetype == 'avro':
        df = spark.read.format('avro').load(path)
    elif filetype == 'txt':
        df = spark.read.csv(path, sep=config['options']['delimiter'], header=config['options']['header'])

    return df


@pytest.fixture
def read_data(read_config, spark_session, request):
    config_data = read_config
    spark = spark_session
    source_config = config_data['source']
    target_config = config_data['target']
    validations_config = config_data['validations']
    dir_path = request.node.fspath.dirname

    if source_config['type'] == 'database':
        source_df = read_db()
    else:
        source_df = read_file(spark=spark, config=source_config, dir_path=dir_path)

    if target_config['type'] == 'database':
        target_df = read_db()
    else:
        target_df = read_file(spark=spark, config=target_config, dir_path=dir_path)

    return source_df, target_df
