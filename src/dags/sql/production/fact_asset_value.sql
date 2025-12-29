DROP TABLE IF EXISTS production.fact_asset_value;

CREATE TABLE production.fact_asset_value AS (
    SELECT
        v.forex_id as asset_id,
        d.month_id as date_id,
        v.value_open as "open",
        v.value_high as high,
        v.value_low as low,
        v.value_close as "close",
        NULL AS volume,
        NULL::NUMERIC as dividends
    FROM staging.forex_value v
    JOIN production.dim_month_date d ON d.month_date = v.date
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
        NULL as dividends
    FROM staging.future_value v
    JOIN production.dim_month_date d ON d.month_date = v.date
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
        v.dividends as dividends
    FROM staging.index_value v
    JOIN production.dim_month_date d ON d.month_date = v.date
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
        NULL as dividends
    FROM staging.crypto_value v
    JOIN production.dim_month_date d ON d.month_date = v.date
    JOIN production.dim_asset_info ai ON ai.asset_id = v.crypto_id
)