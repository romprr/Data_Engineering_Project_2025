DROP TABLE IF EXISTS production.fact_asset_value;

CREATE TABLE production.fact_asset_value AS (
    WITH asset_values AS (
        SELECT
        v.forex_id as asset_id,
        d.month_id as date_id,
        v.value_open as "open",
        v.value_high as high,
        v.value_low as low,
        v.value_close as "close",
        NULL AS volume,
        NULL::NUMERIC as dividends,
        v.region
    FROM staging.forex_clean_value v
    JOIN production.dim_month_date d ON d.month_date = v.clean_date
    JOIN production.dim_asset_info ai ON ai.asset_id = v.forex_id
    
    UNION ALL

    SELECT
        v.future_id as asset_id,
        d.month_id as date_id,
        v.value_open as "open",
        v.value_high as high,
        v.value_low as low,
        v.value_close as "close",
        v.volume AS volume,
        NULL as dividends,
        v.region
    FROM staging.future_clean_value v
    JOIN production.dim_month_date d ON d.month_date = v.clean_date
    JOIN production.dim_asset_info ai ON ai.asset_id = v.future_id

    UNION ALL

    SELECT
        v.index_id as asset_id,
        d.month_id as date_id,
        v.value_open as "open",
        v.value_high as high,
        v.value_low as low,
        v.value_close as "close",
        v.volume AS volume,
        v.dividends as dividends,
        v.region
    FROM staging.index_clean_value v
    JOIN production.dim_month_date d ON d.month_date = v.clean_date
    JOIN production.dim_asset_info ai ON ai.asset_id = v.index_id

    UNION ALL

    SELECT
        v.crypto_id as asset_id,
        d.month_id as date_id,
        v.value_open as "open",
        v.value_high as high,
        v.value_low as low,
        v.value_close as "close",
        v.volume AS volume,
        NULL as dividends,
        v.region
    FROM staging.crypto_clean_value v
    JOIN production.dim_month_date d ON d.month_date = v.clean_date
    JOIN production.dim_asset_info ai ON ai.asset_id = v.crypto_id
    )

    SELECT
        av.asset_id,
        av.date_id,
        av.open,
        av.high,
        av.low,
        av.close,
        av.volume,
        av.dividends,
        r.region_id

    from asset_values av
    JOIN production.dim_region r ON r.region = av.region
)