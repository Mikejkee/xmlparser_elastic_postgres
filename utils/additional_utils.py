import json
from typing import Any
from collections import defaultdict
from uuid import uuid4

import pandas as pd
from lxml import etree

max_value = 2**63 - 1
min_value = -2**63


def post_processing_offer_df(offer_df: pd.DataFrame) -> pd.DataFrame:
    """
        Убирает выходы значений DataFrame в product_id и barcode за bigint.

        :param offer_df: DataFrame.
        :return: Отфильтрованный DataFrame.
        """
    if not offer_df.empty:
        offer_df = offer_df[
            (offer_df['product_id'] <= max_value) & (offer_df['product_id'] >= min_value) &
            (offer_df['barcode'] <= max_value) & (offer_df['barcode'] >= min_value)
            ]
    return offer_df


def assign_levels(category_map: dict) -> None:
    """
    Присваивает уровни категориям на основе их родительских ID.

    :param category_map: Словарь с категориями.
    """
    for cat_id in category_map:
        level = 1  # Начинаем с уровня 1 для корневой категории
        current_id = category_map[cat_id]['parentId']

        # Поднимаемся по дереву категорий
        while current_id and current_id in category_map:
            level += 1
            current_id = category_map[current_id]['parentId']

        category_map[cat_id]['level'] = level


def group_categories_by_level(category_map: dict) -> dict:
    """
    Группирует категории по их уровням.

    :param category_map: Словарь с категориями.
    :return: Словарь, где ключами являются уровни, а значениями — списки категорий.
    """
    levels = defaultdict(list)

    for data in category_map.values():
        levels[data['level']].append(data)

    return dict(levels)


def parse_categories(file_path: str) -> dict:
    """
    Парсит категории из XML файла.

    :param file_path: Путь к XML файлу с категориями и товарами.
    :return: Словарь с категориями по уровням.
    """

    category_map = {}
    context = etree.iterparse(file_path, tag='category', events=('start', ))
    for event, elem in context:
        cat_id = elem.get('id')
        parent_id = elem.get('parentId')
        name = elem.text.strip()

        category_map[cat_id] = {
            'name': name,
            'parentId': parent_id,
            'level': None,
            'categoryId': cat_id
        }

        elem.clear()

    # Определяем уровень каждой категории
    assign_levels(category_map)

    # Формируем итоговый словарь с уровнями
    return group_categories_by_level(category_map)


def fill_category_levels(category_id: str, category_map: dict) -> dict:
    """
    Заполняет поля category_lvl_1, category_lvl_2, category_lvl_3 и category_remaining.

    :param category_id: ID категории.
    :param category_map: Словарь с категориями.
    :return: Словарь с заполненными уровнями.
    """
    result = {f'category_lvl_{i}': None for i in range(1, 4)}
    result['category_remaining'] = None

    current_id = category_id
    level = 4

    while current_id and 4 >= level > 0:
        if current_id in category_map:
            if level <= 3:
                result[f'category_lvl_{level}'] = category_map[current_id]['name']
            else:
                result['category_remaining'] = category_map[current_id]['name']
            current_id = category_map[current_id]['parentId']
            level -= 1
        else:
            break

    return result


def process_offer(offer: Any, category_map: dict) -> dict:
    """
        Функция парсит товар.

        :param offer: XML-элемент товара.
        :param category_map: Словарь с информацией о категориях.
        :return: Словарь.
    """
    old_price = float(offer.findtext('oldprice', 0.0))
    new_price = float(offer.findtext('price', 0.0))
    discount = round((old_price - new_price) / old_price * 100, 2) if old_price != 0 else 0

    categories_levels = fill_category_levels(str(offer.findtext('categoryId', 0)), category_map)
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
        'features': json.dumps(params),
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
    offer_data.update(categories_levels)

    return offer_data
