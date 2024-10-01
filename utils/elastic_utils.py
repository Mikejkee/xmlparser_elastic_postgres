import pandas as pd
from elasticsearch import Elasticsearch, NotFoundError, ApiError, helpers

from .additional_utils import post_processing_offer_df


class SimilarProductsESUpdater:
    def __init__(self, index_name: str, elastic_host: str, elastic_port: str, elastic_pass: str):
        self.es = Elasticsearch(
            [f"http://{elastic_host}:{elastic_port}"],
            basic_auth=('elastic', elastic_pass),
        )
        self.index_name = index_name

    def create_index(self) -> None:
        """Создает индекс с заданным маппингом."""
        mapping = {
            "mappings": {
                "properties": {
                    "uuid": {"type": "keyword"},
                    "title": {"type": "text"},
                    "description": {"type": "text"},
                }
            }
        }

        if not self.es.indices.exists(index=self.index_name):
            self.es.indices.create(index=self.index_name, body=mapping)
            print(f"Индекс '{self.index_name}' создан.")
        else:
            print(f"Индекс '{self.index_name}' уже существует.")

    def load_data_to_elasticsearch(self, load_data: list) -> None:
        """Загружает данные из DataFrame в Elasticsearch."""
        # Подготовка данных для загрузки
        load_data_df = post_processing_offer_df(pd.DataFrame(load_data))

        actions = []
        for _, product in load_data_df.iterrows():
            action = {
                "_index": self.index_name,
                "_id": product.uuid,
                "_source": {
                    'title': product.title,
                    'description': product.description,
                    'uuid': product.uuid
                }
            }
            actions.append(action)

        try:
            helpers.bulk(self.es, actions, raise_on_error=True)
        except helpers.BulkIndexError as e:
            print(f"Ошибка при загрузке данных в индекс: {e}")
            print(f"Ошибки в документах: {e.errors}")

    def find_similar_products(self, product_uuid: str, size: int = 5) -> list:
        """Находит похожие товары по ID товара."""
        try:
            response = self.es.search(index=self.index_name, body={
                "query": {
                    "more_like_this": {
                        "fields": ["title", "description"],
                        "like": [
                            {
                                "_index": self.index_name,
                                "_id": product_uuid
                            }
                        ],
                        "min_term_freq": 1,
                        "max_query_terms": 12,
                    }
                },
                "size": size
            })
            similar_uuids = [hit['_source']['uuid'] for hit in response['hits']['hits']]
            return similar_uuids

        except NotFoundError:
            print(f"Товар с UID {product_uuid} не найден.")
            return []
        except ApiError as e:
            print(f"Ошибка во время поиска похожего товара: {e}")
            return []
