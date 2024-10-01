import os
from uuid import uuid4

from dotenv import load_dotenv
from lxml import etree
from elasticsearch import Elasticsearch

from utils.db_utils import batch_df_in_db
from utils.elastic_utils import create_index, load_data_to_elasticsearch

load_dotenv('.env-test')
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


def process_offer(offer) -> dict:
    old_price = float(offer.findtext('oldprice', 0.0))
    new_price = float(offer.findtext('price', 0.0))
    discount = round((old_price - new_price) / old_price * 100, 2) if old_price != 0 else 0

    params = {param.get('name'): param.text for param in offer.findall('param')}

    offer_data = {
        'uuid': uuid4(),
        'marketplace_id': int(offer.findtext('group_id', 0)),
        'product_id': int(offer.get('id', 0)),
        'title': offer.findtext('name'),
        'description': offer.findtext('description'),
        'brand': offer.findtext('vendor'),
        'seller_id': int(offer.findtext('seller_id', 0)),
        'seller_name': offer.findtext('seller_name'),
        'first_image_url': offer.findtext('picture'),
        'category_id': int(offer.findtext('categoryId', 0)),
        'rating_count': int(offer.findtext('rating_count', 0)),
        'rating_value': float(offer.findtext('rating_value', 0.0)),
        'price_before_discounts': old_price,
        'discount': discount,
        'price_after_discounts': new_price,
        'bonuses': int(offer.findtext('bonuses', 0)),
        'sales': int(offer.findtext('sales', 0)),
        'currency': offer.findtext('currencyId'),
        'barcode': int(offer.findtext('barcode', 0)),
        'similar_sku': [],
    }

    return offer_data


def match_elastic_offer(file_path: str):
    """
    Обрабатывает XML файл чанками и загружает данные в БД.
    """
    es = Elasticsearch(
        [f"http://{ELASTIC_HOST}:{ELASTIC_PORT}"],
        basic_auth=('elastic', ELASTIC_PASSWORD),
    )
    create_index(es, 'offer_index')

    context = etree.iterparse(file_path, tag='offer', events=('end',))

    batch_size = 1000
    batch_data = []

    for event, offer in context:
        offer_data = process_offer(offer)
        batch_data.append(offer_data)

        if len(batch_data) >= batch_size:
            batch_df_in_db(batch_data, config, DB_SCHEMA, DB_TABLE)
            load_data_to_elasticsearch(batch_data, es, 'offer_index')

        offer.clear()
        while offer.getprevious() is not None:
            del offer.getparent()[0]

    # Загружаем оставшиеся данные, если они есть
    if batch_data:
        batch_df_in_db(batch_data, config, DB_SCHEMA, DB_TABLE)
        load_data_to_elasticsearch(batch_data, es, 'offer_index')

    return True


if __name__ == '__main__':
    path = os.path.join('test', 'elektronika_products_20240924_091654.xml')
    match_elastic_offer(path)
