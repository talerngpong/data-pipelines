from logging import Logger
from typing import Optional, Tuple

from airflow.providers.postgres.hooks.postgres import PostgresHook

from helpers.sql_queries import SqlQueries


class DataQualityHelper:
    @staticmethod
    def check_tables_for_non_empty_rows(
            redshift_hook: PostgresHook,
            logger: Logger
    ) -> None:
        for query in SqlQueries.data_quality_check_queries:
            result_tuple: Optional[Tuple] = redshift_hook.get_first(sql=query.value)
            assert result_tuple is not None

            (number_of_rows, *_) = result_tuple
            assert isinstance(number_of_rows, int)

            if number_of_rows > 0:
                logger.info(
                    f'Successfully table named `{query.table_name}` ' +
                    f'(without backticks) with number of rows = {number_of_rows}'
                )
            else:
                error_message: str = f'Failed check on table named `{query.table_name}` ' + \
                    f'(without backticks) with number of rows = {number_of_rows}'
                logger.error(error_message)
                raise AssertionError(error_message)
