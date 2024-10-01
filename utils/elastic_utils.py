import pandas as pd
from elasticsearch import helpers

from utils.additional_utils import post_processing_offer_df


def create_index(es, index_name):
    """Создает индекс с заданным маппингом."""
    mapping = {
        "mappings": {
            "properties": {
                "uuid": {"type": "text"},
                "title": {"type": "text"},
                "description": {"type": "text"},
            }
        }
    }

    if not es.indices.exists(index=index_name):
        es.indices.create(index=index_name, body=mapping)
        print(f"Индекс '{index_name}' создан.")
    else:
        print(f"Индекс '{index_name}' уже существует.")


def load_data_to_elasticsearch(data, es, es_index):
    """Загружает данные из DataFrame в Elasticsearch."""
    # TODO: намутить нужный мэтч по заданию
    #       Расфосовать функии по файликам, зарефакторить код (переменные, типы, оптимизация)
    # Подготовка данных для загрузки

    df = post_processing_offer_df(pd.DataFrame(data))

    actions = []
    for _, product in df.iterrows():
        action = {
            "_index": es_index,
            "_id": product.uuid,
            "_source": {
                'title': product.title,
                'description': product.description,
                'uuid': product.uuid
            }
        }
        actions.append(action)

    try:
        helpers.bulk(es, actions, raise_on_error=True)
    except helpers.BulkIndexError as e:
        print(f"Ошибка при загрузке данных в индекс: {e}")
        print(f"Ошибки в документах: {e.errors}")
