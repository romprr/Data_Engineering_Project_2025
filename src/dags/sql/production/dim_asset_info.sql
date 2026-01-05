DROP TABLE IF EXISTS production.dim_asset_info;

CREATE TABLE production.dim_asset_info AS (
    SELECT
        crypto_id as asset_id,
        symbol,
        NULL as asset_currency,
        from_currency as asset_from_currency,
        to_currency as asset_to_currency,
        crypto_name as name,
        region as asset_region,
        'crypto' as asset_type
    FROM staging.crypto_info

    UNION ALL

    SELECT
        forex_id as asset_id,
        symbol,
        NULL as asset_currency,
        from_currency as asset_from_currency,
        to_currency as asset_to_currency,
        forex_name as name,
        region as asset_region,
        'forex' as asset_type
    FROM staging.forex_info

    UNION ALL

    SELECT
        index_id as asset_id,
        symbol,
        currency as asset_currency,
        NULL as asset_from_currency,
        NULL as asset_to_currency,
        index_name as name,
        region as asset_region,
        'index' as asset_type
    FROM staging.index_info

    UNION ALL

    SELECT
        future_id as asset_id,
        symbol,
        currency as asset_currency,
        NULL as asset_from_currency,
        NULL as asset_to_currency,
        futures_name as name,
        region as asset_region,
        'future' as asset_type
    FROM staging.future_info
)