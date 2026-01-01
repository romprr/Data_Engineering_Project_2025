DROP TABLE IF EXISTS  staging.future_value;

CREATE TABLE staging.future_value AS (
    SELECT
        'future_' || hs.symbol || '_asset' AS future_id,
        TO_TIMESTAMP(hs.value_timestamp::INT)::DATE AS "date",
        hs.value_open,
        hs.value_high,
        hs.value_low,
        hs.value_close,
        hs.volume,
        c.region
    FROM raw.FUTURES_HISTORY hs
    JOIN staging.future_info c ON c.symbol = hs.symbol
    JOIN staging.assets_region_translation rt ON rt.exchange_timezone = c.exchange_timezone
);