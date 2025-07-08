from src.data_validations.records_only_target import records_only_in_target
from src.data_validations.records_only_in_source import records_only_in_source
from src.utility.report_lib import write_output


def count_val(source, target, key_columns):
    source_cnt = source.count()
    target_cnt = target.count()

    if source_cnt == target_cnt:
        status = 'PASS'
        write_output(validation_type="count check",
                     status=status,
                     details=f"Count is matching between source and target. source count{source_cnt} and target count is {target_cnt}")
        # records_only_in_target(source_df=source, target_df=target, key_columns=key_columns)
        # records_only_in_source(source_df=source, target_df=target, key_columns=key_columns)
    else:
        status = 'FAIL'
        write_output(validation_type="count check",
                     status=status,
                     details=f"Count is not matching between source and target. source count{source_cnt} and target count is {target_cnt}")
        print(
            f"count is not matching. source count{source_cnt} and target count is {target_cnt} and diff is {abs(source_cnt - target_cnt)}")

        records_only_in_target(source_df=source, target_df=target, key_columns=key_columns)
        records_only_in_source(source_df=source, target_df=target, key_columns=key_columns)

    return status
