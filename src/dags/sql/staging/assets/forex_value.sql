DROP TABLE IF EXISTS  staging.forex_value;

CREATE TABLE staging.forex_value AS (
    SELECT
        'forex_' || hs.symbol || '_asset' AS forex_id,
        TO_TIMESTAMP(hs.value_timestamp::INT)::DATE AS "date",
        hs.value_open,
        hs.value_high,
        hs.value_low,
        hs.value_close,
        rt.region
    FROM raw.FOREX_HISTORY hs
    JOIN staging.forex_info c ON c.symbol = hs.symbol
    JOIN staging.assets_region_translation rt ON rt.exchange_timezone = c.exchange_timezone
);