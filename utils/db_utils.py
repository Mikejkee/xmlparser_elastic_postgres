import logging

import pandas as pd
import sqlalchemy as sa
from sqlalchemy.exc import ResourceClosedError
from sqlalchemy import text

from .sql_processor import SQLProcessor
from .additional_utils import post_processing_offer_df

sql_processor = SQLProcessor()
logger = logging.getLogger()


def load_data_from_bd(config: dict,
                      name_sql_file: str,
                      base_dir: str,
                      schema: str,
                      table_name: str,
                      params_names: object = None,
                      params_values: object = None,
                      name_sql_dir: str = 'sql_query_files',
                      expanding: bool = True) -> bool | pd.DataFrame:
    try:
        print(f'->Выполняем подключение к бд и выгрузку из таблицы - {schema}.{table_name} <-')
        sql_processor.extract_settings_url = (
            f'{config["psql_conn_type"]}ql+psycopg2://'
            f'{config["psql_login"]}:{config["psql_password"]}'
            f'@{config["psql_hostname"]}:{config["psql_port"]}'
            f'/{config["psql_name_bd"]}'
        )

        extract_query = sql_processor.get_query_from_sql_file(
            name_sql_file,
            base_dir,
            query_dir=name_sql_dir,
            params_names=params_names,
            params_values=params_values,
            expanding=expanding,
        )

        sql_processor.create_extract_engine()
        with sql_processor.extract_settings_connect() as connection:
            try:
                data = sql_processor.extract_data_sql(
                    extract_query,
                    connection=connection,
                    params=params_values,
                )
                print(f'->Выгрузка из таблицы - {schema}.{table_name} - успешна <-')
                return data

            except ResourceClosedError:
                connection.commit()
                connection.detach()
                print(f'->Обновление таблицы - {schema}.{table_name} - успешно <-')
                return True

    except Exception as e:
        logger.error(f'->Ошибка {e} при выгрузке данных из таблицы - {schema}.{table_name} <-')
        raise e


def load_data_in_db(df: pd.DataFrame,
                    config: dict,
                    schema: str,
                    name_table_in_db: str,
                    exists='append',
                    index=False,
                    ):
    try:
        print(f'->Вставляем записи в таблицу - {schema}.{name_table_in_db}  <-')

        sql_processor.load_settings_url = (
            f'{config["psql_conn_type"]}ql+psycopg2://'
            f'{config["psql_login"]}:{config["psql_password"]}'
            f'@{config["psql_hostname"]}:{config["psql_port"]}'
            f'/{config["psql_name_bd"]}'
        )

        sql_processor.create_load_engine()
        with sql_processor.load_settings_connect() as connection:
            sql_processor.load_data_sql(df, name_table_in_db, exists, connection=connection, index=index, schema=schema)
            print(f'->Записи в таблице - {schema}.{name_table_in_db} созданы <-')
            connection.detach()

    except Exception as e:
        logger.error(f'->Ошибка {e} при загрузке данных в таблицу - {schema}.{name_table_in_db} <-')
        raise e


def update_solo_data_in_db(config: dict,
                           name_sql_file: str,
                           base_dir: str,
                           schema: str,
                           table_name: str,
                           params_names: object = None,
                           params_values: object = None,
                           name_sql_dir: str = 'sql_query_files',
                           expanding: bool = True):
    try:
        print(f'->Обновляем записи в таблицу - {schema}.{table_name}  <-')

        sql_processor.extract_settings_url  = (
            f'{config["psql_conn_type"]}ql+psycopg2://'
            f'{config["psql_login"]}:{config["psql_password"]}'
            f'@{config["psql_hostname"]}:{config["psql_port"]}'
            f'/{config["psql_name_bd"]}'
        )

        update_query = sql_processor.get_query_from_sql_file(
            name_sql_file,
            base_dir,
            query_dir=name_sql_dir,
            params_names=params_names,
            params_values=params_values,
            expanding=expanding,
        )

        sql_processor.create_extract_engine()
        with sql_processor.extract_settings_connect() as connection:
            sql_processor.extract_data_sql(
                update_query,
                connection=connection,
                params=params_values,
            )
            connection.commit()
            connection.detach()
            print(f'->Обновление в таблице - {schema}.{table_name} - успешна <-')
            return True

    except Exception as e:
        logger.error(f'->Ошибка {e} при загрузке данных в таблицу - {schema}.{table_name} <-')
        print(f'->Ошибка {e} при загрузке данных в таблицу - {schema}.{table_name} <-')
        raise e


def batch_df_in_db(batch_data: list,
                   config: dict,
                   schema: str,
                   name_table_in_db: str,
                   exists='append',
                   index=False) -> None:
    df = post_processing_offer_df(pd.DataFrame(batch_data))
    load_data_in_db(df, config, schema, name_table_in_db, exists, index)
    batch_data.clear()


def load_data_from_bd_chunk_function(config: dict,
                                     name_sql_file: str,
                                     base_dir: str,
                                     schema: str,
                                     table_name: str,
                                     process_function: callable,
                                     chunk_size: int = 30000,
                                     params_names: object = None,
                                     params_values: object = None,
                                     name_sql_dir: str = 'sql_query_files',
                                     expanding: bool = True,
                                     *args, **kwargs) -> None:
    try:
        print(f'->Выполняем подключение к бд и выгрузку из таблицы - {schema}.{table_name} <-')

        sql_processor.extract_settings_url = (
            f'{config["psql_conn_type"]}ql+psycopg2://'
            f'{config["psql_login"]}:{config["psql_password"]}'
            f'@{config["psql_hostname"]}:{config["psql_port"]}'
            f'/{config["psql_name_bd"]}'
        )

        extract_data_sql_query = sql_processor.get_query_from_sql_file(
            name_sql_file,
            base_dir,
            query_dir=name_sql_dir,
            params_names=params_names,
            params_values=params_values,
            expanding=expanding,
        )

        sql_processor.create_extract_engine()
        with sql_processor.extract_settings_connect() as connection:
            for chunk_df in sql_processor.extract_data_sql(extract_data_sql_query,
                                                           params=params_values,
                                                           connection=connection,
                                                           chunksize=chunk_size):
                process_function(chunk_df, *args, **kwargs)

    except Exception as e:
        print(f'->Ошибка {e} при выгрузке данных из таблицы - {schema}.{table_name} <-')
        raise e
