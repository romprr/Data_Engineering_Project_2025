DROP TABLE IF EXISTS index_value;

CREATE TABLE index_value AS (
    SELECT
        'index_' || i.symbol || '_value' AS index_id,
        i.value_timestamp AS "timestamp",
        i.value_open,
        i.value_high,
        i.value_low,
        i.value_close,
        i.volume,
        i.dividends
    FROM INDEX_HISTORY i 
);