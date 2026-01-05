DROP TABLE IF EXISTS  staging.index_value;

CREATE TABLE staging.index_value AS (
    SELECT
        'index_' || hs.symbol || '_asset' AS index_id,
        TO_TIMESTAMP(hs.value_timestamp::INT) AT TIME ZONE c.exchange_timezone AS "date",
        hs.value_open,
        hs.value_high,
        hs.value_low,
        hs.value_close,
        hs.volume,
        hs.dividends,
        rt.region
    FROM raw.INDEX_HISTORY hs 
    JOIN staging.index_info c ON c.symbol = hs.symbol
    JOIN staging.assets_region_translation rt ON rt.exchange_timezone = c.exchange_timezone
    WHERE NOT 'NaN' = ANY(ARRAY[hs.value_open, hs.value_high, hs.value_low, hs.value_close]) AND
    EXTRACT(DAY FROM (TO_TIMESTAMP(hs.value_timestamp::INT) AT TIME ZONE c.exchange_timezone)::DATE) = 1

);