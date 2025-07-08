from src.data_validations.count_check import count_val


def test_count(read_data, read_config):
    source_df, target_df = read_data
    config = read_config
    # print("source df")
    # source_df.show()
    # source_df.printSchema()
    # print(source_df.count())
    # print("target df")
    # target_df.show()
    # target_df.printSchema()
    key_columns = config['validations']['count_check']['key_columns']
    status = count_val(source=source_df, target=target_df, key_columns=key_columns)

    assert status == 'PASS'
