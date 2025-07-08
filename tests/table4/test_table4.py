from src.data_validations.count_check import count_val


def test_count(read_data):
    source_df, target_df = read_data
    print("source df")
    source_df.show()
    source_df.printSchema()
    print(source_df.count())
    print("target df")
    target_df.show()
    target_df.printSchema()
    status = count_val(source_df=source_df, target_df=target_df) #FAIL

    assert status == 'PASS'
