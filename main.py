import os

import pandas as pd
from lxml import etree

from config_file import config, base_dir, ELASTIC_HOST, ELASTIC_PORT, ELASTIC_PASSWORD, DB_TABLE, DB_SCHEMA
from utils.db_utils import batch_df_in_db, load_data_from_bd_chunk_function, update_solo_data_in_db, load_data_from_bd
from utils.elastic_utils import SimilarProductsESUpdater
from utils.additional_utils import process_offer, parse_categories


def update_product_with_similar_db(chunk_df: pd.DataFrame, updater: SimilarProductsESUpdater) -> bool:
    """
        Обновляет документы товара в базе данных, добавляя информацию о похожих товарах.

        :param chunk_df: DataFrame.
        :param updater: SimilarProductsESUpdater.

        :return: True, если обработка завершена успешно.
    """

    base_dir_utils = os.path.join(base_dir, 'utils')

    for _, row in chunk_df.iterrows():
        similar_uuids = updater.find_similar_products(row.uuid)

        if not similar_uuids:
            print(f"Product with ID {row.uuid} not found for update.")
            continue

        update_solo_data_in_db(
            config,
            'update_similar_sku.sql',
            base_dir_utils,
            DB_SCHEMA,
            DB_TABLE,
            params_values={
                'uuid': row.uuid,
                "similar_sku": similar_uuids
            },
            expanding=False
        )
    return True


def match_elastic_offer(file_path: str, batch_size: int = 10000):
    """
        Обрабатывает XML файл чанками и загружает данные о товарах сначала в бд, далее простраивает
        индекс для Elasticsearch, ищет похожие товары друг между другом и обновляет информацию о них в бд.

        :param file_path: Путь к XML файлу, содержащему информацию о товарах.
        :param batch_size: Размер чанков.

        :return: True, если обработка завершена успешно.
    """

    elastic_updater = SimilarProductsESUpdater('offer_index', ELASTIC_HOST, ELASTIC_PORT, ELASTIC_PASSWORD)
    elastic_updater.create_index()

    categories_by_level = parse_categories(file_path)
    category_map = {data['categoryId']: data for level_data in categories_by_level.values() for data in level_data}

    batch_data = []
    context = etree.iterparse(file_path, tag='offer', events=('end',))
    for event, offer in context:
        offer_data = process_offer(offer, category_map)
        batch_data.append(offer_data)

        if len(batch_data) >= batch_size:
            elastic_updater.load_data_to_elasticsearch(batch_data)
            batch_df_in_db(batch_data, config, DB_SCHEMA, DB_TABLE)
            batch_data.clear()

        offer.clear()
        while offer.getprevious() is not None:
            del offer.getparent()[0]

    # Загружаем оставшиеся данные, если они есть
    if batch_data:
        elastic_updater.load_data_to_elasticsearch(batch_data)
        batch_df_in_db(batch_data, config, DB_SCHEMA, DB_TABLE)
        batch_data.clear()

    base_dir_utils = os.path.join(base_dir, 'utils')
    load_data_from_bd_chunk_function(
        config,
        'select_from_sku.sql',
        base_dir_utils,
        DB_SCHEMA,
        DB_TABLE,
        update_product_with_similar_db,
        updater=elastic_updater
    )

    return True


if __name__ == '__main__':
    path = os.path.join('test', 'elektronika_products_20240924_091654.xml')
    match_elastic_offer(path)
