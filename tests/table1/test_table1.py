# def test_spark(spark_session):
#     assert True


def test_count(read_config):
    config_data = read_config
    print("config data is", config_data, type(config_data), end='\n')
    print("source config is", config_data['source'],end='\n')
    print("target config is", config_data['target'],end='\n')
    print("validation config is", config_data['validations'],end='\n')

