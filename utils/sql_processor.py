import configparser
import email
import imaplib
import io
import locale
import os
import textwrap
from dataclasses import dataclass
from datetime import datetime, timedelta
from email.header import decode_header
from ftplib import FTP_TLS, FTP

import pandas as pd
import sqlalchemy as sa
from pandas import DataFrame
from pathlib import Path
from typing import Any, List, Union


@dataclass
class SQLProcessor:
    """Базовый класс для соединения с БД и SQL-запросов
        COMMON - методы для любого логического блока;
        EXTRACT - методы выгрузки данных из БД;
        LOAD - методы загрузки данных в БД;
    """

    extract_settings_url: str = None
    region_settings_url: str = None
    load_settings_url: str = None
    settings_url: str = None

    extract_settings_engine: object = None
    region_settings_engine: object = None
    load_settings_engine: object = None
    settings_engine: sa.engine.Engine = None

    extract_settings_connection: object = None
    region_settings_connection: object = None
    load_settings_connection: object = None
    settings_connection: sa.engine.Connection = None

    inside_logger: object = None

    @staticmethod
    def config(file_dir: str, file_name: str) -> dict:
        """COMMON Метод осуществляет парсинг конфигурационного файла и возвращает словарь
           :param str file_dir: название директории файлов
           :param str file_name: название файла
        """
        common_dir = Path(os.path.dirname(os.path.realpath(__file__))).parent.parent
        need_path = os.path.join(common_dir, file_dir, file_name)
        result = dict()
        config = configparser.ConfigParser()
        config.read(need_path)
        auth, data, mode = config['AUTH'], config['DATA'], config['MODE']
        for key, value in auth.items():
            result.update({key: value})
        for key, value in data.items():
            result.update({key: value})
        for key, value in mode.items():
            result.update({key: value})
        return result

    # COMMON LOGIC
    @staticmethod
    def guess_encoding(file_path: str) -> str:
        """ COMMON Метод определяет кодировку файла SQL-запроса
            :param str file_path: Путь к файлу
        """
        with io.open(file_path, "rb") as f:
            data = f.read(5)
        if data.startswith(b"\xEF\xBB\xBF"):
            return "utf-8-sig"
        elif data.startswith(b"\xFF\xFE") or data.startswith(b"\xFE\xFF"):
            return "utf-16"
        else:
            try:
                with io.open(file_path, encoding="utf-8"):
                    return "utf-8"
            except:
                return locale.getdefaultlocale()[1]

    @staticmethod
    def get_query_from_sql_file(file_name: str, base_dir: str, params_names: object = None,
                                params_values: object = None, expanding: bool = True,
                                query_dir: str = 'sql_queries') -> str:
        """ COMMON Метод возвращает SQL-запрос в строковом виде из SQL-файла
            :param str file_name: Название файла SQL
            :param str base_dir: Путь к базовой директории проекта
            :param object params_names: Параметры табличных имен запроса - строка, словарь или кортеж
            :param object params_values: Параметры значений запроса - список, кортеж или словарь
            :param bool expanding: Параметр expanding для sa.bindparam
            :param str query_dir: Директория с запросами
        """
        need_path = os.path.join(base_dir, query_dir, file_name)
        with open(need_path, 'r', encoding=SQLProcessor.guess_encoding(need_path)) as sql_file:
            lines = sql_file.read()
            query_string = textwrap.dedent(f"""{lines}""").replace('?', '{}')
            if params_names:
                if isinstance(params_names, str):
                    query_string = query_string.format(params_names)
                elif isinstance(params_names, dict):
                    query_string = query_string.format(**params_names)
                else:
                    try:
                        query_string = query_string.format(*params_names)
                    except Exception as e:
                        print(e)
                        raise ValueError('Параметры табличных имен запроса не валидны!')

            if params_values:
                query_string = sa.text(query_string)
                names_list = params_values
                if isinstance(params_values, dict):
                    names_list = params_values.keys()
                for key in names_list:
                    query_string = query_string.bindparams(sa.bindparam(key, expanding=expanding))

            return query_string

    def sql_query(self, sql_query: str, connection: object, params: dict = None) -> object:
        """ COMMON Метод выполняет SQL запрос
            :param str sql_query: строка запроса SQL
            :param object connection: объект соединения
            :param dict params: параметры запроса
        """
        if params:
            return connection.execute(sql_query, params)
        return connection.execute(sa.text(sql_query))

    def get_max_value(self, name_value: str, table_name: str, connection: object, url: str = None,
                      db_name: str = None, default_value: Any = 0) -> Any:
        """ COMMON Метод вычисляет максимальное значение столбца таблицы
            :param str name_value: название вычисляемого максимального значения
            :param str table_name: название таблицы
            :param object connection: объект соединения
            :param str url: url источника
            :param str db_name: название БД
            :param Any default_value: значение по-умолчанию
        """
        if isinstance(default_value, str):
            default_value = "'" + default_value + "'"
        if url and db_name:
            max_value_df = pd.read_sql(f'SELECT COALESCE(MAX({name_value}), {default_value}) AS max_val FROM "{table_name}" '
                                       f"WHERE source_url = '{url}' AND source_db = '{db_name}';",
                                       connection)
        else:
            max_value_df = pd.read_sql(f'SELECT COALESCE(MAX({name_value}), {default_value}) AS max_val FROM "{table_name}";',
                                       connection)
        max_value = max_value_df['max_val'][0]
        return max_value

    def create_connection(self, dialect: str = None, driver: str = None, login: str = None, password: str = None,
                          host: str = None, port: int = None, db: str = None, url: str = None,
                          **kwargs) -> sa.engine.Connection:
        """ COMMON Метод создает engine для соединения с БД.
            :param str dialect: диалект SQL
            :param str driver: драйвер SQL
            :param str login: логин для соединения
            :param str password: пароль для соединения
            :param str host: host сервера
            :param int port: port сервера (необязательный,
            при отсутствии будет использоваться 3306 для MySQL и 5432 для PostgreSQL)
            :param str db: имя базы данных
            :param str url: готовый url (если есть). Используется, как предпочтительный
        """
        if url:
            self.settings_url = url
        else:
            if dialect is None or driver is None or login is None or password is None or host is None or db is None:
                raise ValueError("Since 'url' is not specified, arguments 'dialect', 'driver', 'login', 'password', "
                                 "'host' and 'db' are required!")
            if dialect == 'mysql' and not port:
                port = 3306
            if dialect == 'postgresql' and not port:
                port = 5432
            self.settings_url = f"{dialect}+{driver}://{login}:{password}@{host}:{port}/{db}"

        self.current_logger.info(f'Prepare variables: url: {self.settings_url}')
        self.settings_engine = sa.create_engine(self.settings_url, **kwargs)
        self.settings_connection = self.settings_engine.connect()
        return self.settings_connection

    # EXTRACT LOGIC
    def create_extract_engine(self, **kwargs) -> object:
        """EXTRACT Метод создает engine для соединения с БД"""
        self.extract_settings_engine = sa.create_engine(self.extract_settings_url, **kwargs)
        return self.extract_settings_engine

    def extract_settings_connect(self) -> object:
        """EXTRACT Метод создает соединение с БД"""
        self.extract_settings_connection = self.extract_settings_engine.connect()
        return self.extract_settings_connection

    def extract_data_sql(self, sql_query: str, params: dict = None, connection: object = None, chunksize: int = None, parse_dates: bool = True) -> DataFrame:
        """ EXTRACT Метод осуществляет чтение SQL-запроса или таблицы базы данных в DataFrame
            :param str sql_query: строка запроса SQL
            :param dict params: параметры для read_sql pandas
            :param object connection: объект соединения
            :param int chunksize: размер пакета
            :param bool parse_dates: парсинг дат
        """
        current_connection = self.extract_settings_connection
        if connection:
            current_connection = connection
        return pd.read_sql(sql_query, current_connection, params=params, chunksize=chunksize, parse_dates=parse_dates)

    def get_check_list(self, check_field: str, table_name: str) -> List:
        """ EXTRACT Метод выгружает список уже загруженных файлов в DataFrame
            :param str check_field: столбец для проверки
            :param str table_name: название таблицы
        """
        with self.extract_settings_connect() as connection:
            self.sql_query('SET SCHEMA \'public\'', connection)
            filename_df = self.extract_data_sql('SELECT ' + check_field + ' FROM "' + table_name + '"')
            check_list = filename_df[check_field].to_list()

            connection.detach()
        connection.close()

        return check_list

    def create_load_engine(self, **kwargs) -> object:
        """LOAD Метод создает engine для соединения с БД"""
        self.load_settings_engine = sa.create_engine(self.load_settings_url, **kwargs)
        return self.load_settings_engine

    def load_settings_connect(self) -> object:
        """LOAD Метод создает соединение с БД"""
        self.load_settings_connection = self.load_settings_engine.connect()
        return self.load_settings_connection

    def load_data_sql(self, dataframe: DataFrame, table: str, if_exists: bool = False, index: bool = False,
                      connection: sa.engine.Connection = None, **kwargs) -> object:
        """ LOAD Метод осуществляет загрузку записей, хранящихся в DataFrame, в БД
            :param if_exists:
            :param index:
            :param DataFrame dataframe: датафрейм
            :param str table: название таблицы
            :param bool truncate: режим загрузки
            :param sa.engine.Connection connection: использует предложенное соединение,
            вместо self.load_settings_connection по умолчанию
        """
        if not connection:
            connection = self.load_settings_connection
        return dataframe.to_sql(table, connection, if_exists=if_exists, index=index, **kwargs)
