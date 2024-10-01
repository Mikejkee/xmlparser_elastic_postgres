UPDATE sku
SET similar_sku = ARRAY(SELECT unnest(:similar_sku)::uuid)
WHERE uuid = :uuid;