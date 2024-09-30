import os

from lxml import etree
from elasticsearch import Elasticsearch, helpers

from utils.db_utils import batch_df_in_db



config = {
    'psql_login': os.environ.get('SQL_USER'),
    'psql_password': os.environ.get('SQL_PASSWORD'),
    'psql_hostname': os.environ.get('SQL_HOST'),
    'psql_port': os.environ.get('SQL_PORT'),
    'psql_name_bd': os.environ.get('SQL_PARSER_DATABASE'),
    'psql_conn_type': 'postgres'
}
db_schema = os.environ.get('DB_SCHEMA')
db_table = os.environ.get('DB_TABLE')
base_dir = os.path.dirname(os.path.abspath(__file__))


def process_offer(offer):
    old_price = float(offer.findtext('oldprice', 0.0))
    new_price = float(offer.findtext('price', 0.0))
    discount = round((old_price - new_price) / old_price * 100, 2) if old_price != 0 else 0

    params = {param.get('name'): param.text for param in offer.findall('param')}

    offer_data = {
        'marketplace_id': int(offer.findtext('group_id', 0)),
        'product_id': int(offer.get('id', 0)),
        'title': offer.findtext('name'),
        'description': offer.findtext('description'),
        'brand': offer.findtext('vendor'),
        'seller_id': int(offer.findtext('seller_id', 0)),
        'seller_name': offer.findtext('seller_name'),
        'first_image_url': offer.findtext('picture'),
        'category_id': int(offer.findtext('categoryId', 0)),
        'features': params,
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


def create_index(es, index_name):
    """Создает индекс с заданным маппингом."""
    mapping = {
        "mappings": {
            "properties": {
                "uuid": {"type": "integer"},
                "title": {"type": "text"},
                "description": {"type": "text"},
            }
        }
    }

    # Проверяем, существует ли индекс, и создаем его, если нет
    if not es.indices.exists(index=index_name):
        es.indices.create(index=index_name, body=mapping)
        print(f"Индекс '{index_name}' создан.")
    else:
        print(f"Индекс '{index_name}' уже существует.")


def load_data_to_elasticsearch(dataframe, es_index, es_host='localhost', es_port=9200):
    """Загружает данные из DataFrame в Elasticsearch."""
    # TODO: разобраться с работой эластика и индексов, вставить данные в эластик, намутить нужный мэтч по заданию
    #       Расфосовать функии по файликам, зарефакторить код (переменные, типы, оптимизация)
    # Создаем клиент Elasticsearch
    es = Elasticsearch([{'host': es_host, 'port': es_port}])

    # Создаем индекс с маппингом
    create_index(es, es_index)

    # Подготовка данных для загрузки
    actions = []
    for _, row in dataframe.iterrows():
        action = {
            "_index": es_index,
            "_id": row['product_id'],  # Используем уникальный идентификатор
            "_source": row.to_dict()
        }
        actions.append(action)

    # Используем Bulk API для загрузки данных
    helpers.bulk(es, actions)
    print(f"Данные успешно загружены в индекс '{es_index}'.")


def match_elastic_offer(file_path: str, config, db_schema, db_table):
    """
    Обрабатывает XML файл чанками и загружает данные в БД.
    """
    context = etree.iterparse(file_path, tag='offer', events=('end',))

    batch_size = 1000
    batch_data = []

    for event, offer in context:
        offer_data = process_offer(offer)
        batch_data.append(offer_data)

        if len(batch_data) >= batch_size:
            batch_df_in_db(batch_data, config, db_schema, db_table)

        offer.clear()
        offer_parent = offer.getparent()

        while offer_parent is not None:
            offer_parent.remove(offer)

    # Загружаем оставшиеся данные, если они есть
    if batch_data:
        batch_df_in_db(batch_data, config, db_schema, db_table)


    return True


if __name__ == '__main__':
    path = os.path.join('test', 'elektronika_products_20240924_091654.xml')
    match_elastic_offer(path)
