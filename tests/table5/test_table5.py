from src.data_validations.count_check import count_val
from src.data_validations.duplicate_check import duplicate_check
from src.data_validations.unique_check import uniqueness_check
from src.data_validations.null_check import null_value_check


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


def test_duplicate(read_data, read_config):
    source_df, target_df = read_data
    config = read_config
    key_columns = config['validations']['duplicate_check']['key_columns']
    status = duplicate_check(df=target_df, key_columns=key_columns)
    assert status == 'PASS'


def test_unique(read_data, read_config):
    source_df, target_df = read_data
    config = read_config
    unique_cols = config['validations']['uniqueness_check']['unique_columns']
    status = uniqueness_check(df=target_df, unique_cols=unique_cols)
    assert status == 'PASS'

def test_null_check(read_data, read_config):
    source_df, target_df = read_data
    config = read_config
    null_cols = config['validations']['null_check']['null_columns']
    status = null_value_check(df=target_df, null_cols=null_cols)
    assert status == 'PASS'


