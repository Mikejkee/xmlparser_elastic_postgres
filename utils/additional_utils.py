import pandas as pd

max_value = 2**63 - 1
min_value = -2**63


def post_processing_offer_df(offer_df: pd.DataFrame) -> pd.DataFrame:
    offer_df = offer_df[
        (offer_df['product_id'] <= max_value) & (offer_df['product_id'] >= min_value) &
        (offer_df['barcode'] <= max_value) & (offer_df['barcode'] >= min_value)
        ]
    return offer_df
