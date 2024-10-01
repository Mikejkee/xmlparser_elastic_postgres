import os

from dotenv import load_dotenv

# load_dotenv('.env-test')
ELASTIC_HOST = os.environ.get('ELASTIC_HOST')
ELASTIC_PORT = os.environ.get('ES_PORT')
ELASTIC_PASSWORD = os.environ.get('ELASTIC_PASSWORD')

DB_TABLE = os.environ.get('DB_TABLE')
DB_SCHEMA = os.environ.get('DB_SCHEMA')
config = {
    'psql_login': os.environ.get('POSTGRES_USER'),
    'psql_password': os.environ.get('POSTGRES_PASSWORD'),
    'psql_hostname': os.environ.get('POSTGRES_HOST'),
    'psql_port': os.environ.get('POSTGRES_PORT'),
    'psql_name_bd': os.environ.get('POSTGRES_DB'),
    'psql_conn_type': 'postgres'
}
base_dir = os.path.dirname(os.path.abspath(__file__))