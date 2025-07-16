from pyspark.sql import SparkSession
import pytest
import yaml
import os
from src.utility.general_utility import flatten
import json
from pyspark.sql.types import StructType
import subprocess


@pytest.fixture(scope="session")
def spark_session():
    eal_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    jar_path = os.path.join(eal_path, "jars", "mssql-jdbc-12.2.0.jre8.jar")
    spark = SparkSession.builder \
        .master("local[1]") \
        .appName("Automation") \
        .config("spark.jars", jar_path) \
        .config("spark.driver.extraClassPath", jar_path) \
        .config("spark.executor.extraClassPath", jar_path) \
        .getOrCreate()
    return spark


@pytest.fixture(scope='module')
def read_config(request):
    dir_path = request.node.fspath.dirname #C:\Users\Admin\PycharmProjects\eal_framework_april_2025\tests\table4
    config_path = os.path.join(dir_path, "config.yml") #C:\Users\Admin\PycharmProjects\eal_framework_april_2025\tests\table4\config.yml
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


def load_credentials(env):
    """Load credentials from the centralized YAML file."""
    eal_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    secrets_file_path = os.path.join(eal_path, "project_config", "secrets.yml")
    with open(secrets_file_path, "r") as file:
        credentials = yaml.safe_load(file)
        print(credentials[env])
    return credentials[env]


def read_db(spark, config, dir_path, env):
    creds = load_credentials(env=env)
    cred_lookup = config['cred_lookup']  # sqlserver
    creds = creds[cred_lookup]
    print("creds", creds)
    if config['transformation'][0].lower() == 'y' and config['transformation'][1].lower() == 'sql':
        sql_query = read_query(dir_path)
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
            df = spark.read.csv(config['path'], header=config['options']['header'],
                                inferSchema=config['options']['inferSchema'])
    elif filetype == 'json':
        df = spark.read.json(path, multiLine=config['options']['multiline'])
        df = flatten(df)
    elif filetype == 'parquet':
        df = spark.read.parquet(path)
    elif filetype == 'avro':
        df = spark.read.format('avro').load(path)
    elif filetype == 'txt':
        df = spark.read.csv(path, sep=config['options']['delimiter'], header=config['options']['header'])

    if config['transformation'] == 'Y':
        df.createOrReplaceTempView('df')
        sql_query = read_query(dir_path)
        df = spark.sql(sql_query)
    return df


@pytest.fixture(scope='module')
def read_data(read_config, spark_session, request, get_env):
    #print(" read_data read_dataread_data hello hello hello hello hello hello hello hello read_data read_data read_data")
    config_data = read_config
    spark = spark_session
    env = get_env
    source_config = config_data['source']
    target_config = config_data['target']
    validations_config = config_data['validations']
    dir_path = request.node.fspath.dirname

    if source_config['transformation'][1].lower() == 'python' and source_config['transformation'][0].lower() == 'y':
        python_file_path = dir_path + '/transformation.py'
        print("python file name", python_file_path)
        subprocess.run(["python", python_file_path])

    if source_config['type'] == 'database':
        source_df = read_db(spark=spark, config=source_config, dir_path=dir_path, env=env)
    else:
        source_df = read_file(spark=spark, config=source_config, dir_path=dir_path)

    if target_config['type'] == 'database':
        target_df = read_db(spark=spark, config=target_config, dir_path=dir_path, env=env)
    else:
        target_df = read_file(spark=spark, config=target_config, dir_path=dir_path)

    return source_df, target_df


@pytest.fixture(scope='session')
def get_env(request):
    return request.config.getoption("--env")




def pytest_addoption(parser):
    parser.addoption(
        "--env",
        action="store",
        default="qa",
        help="Environment to load credentials for (qa, dev, prod)"
    )
