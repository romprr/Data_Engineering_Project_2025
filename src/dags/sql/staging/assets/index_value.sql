DROP TABLE IF EXISTS  staging.index_value;

CREATE TABLE staging.index_value AS (
    SELECT
        'index_' || hs.symbol || '_value' AS index_id,
        TO_TIMESTAMP(hs.value_timestamp::INT) AT TIME ZONE c.exchange_timezone AS "date",
        hs.value_open,
        hs.value_high,
        hs.value_low,
        hs.value_close,
        hs.volume,
        hs.dividends
    FROM raw.INDEX_HISTORY hs 
    JOIN staging.index_info c ON c.symbol = hs.symbol
    WHERE NOT 'NaN' = ANY(ARRAY[hs.value_open, hs.value_high, hs.value_low, hs.value_close]) AND
    EXTRACT(DAY FROM (TO_TIMESTAMP(hs.value_timestamp::INT) AT TIME ZONE c.exchange_timezone)::DATE) = 1

);