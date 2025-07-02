# def test_spark(spark_session):
#     assert True


def test_count(read_config, read_data):
    config_data = read_config
    source_df, target_df = read_data
    print("source df")
    source_df.show()
    source_df.printSchema()
    print(source_df.count())
    print("target df")
    target_df.show()
    target_df.printSchema()


