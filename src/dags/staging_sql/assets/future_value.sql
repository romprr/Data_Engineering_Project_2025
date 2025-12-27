DROP TABLE IF EXISTS  staging.future_value;

CREATE TABLE staging.future_value AS (
    SELECT
        'future_' || hs.symbol || '_value' AS future_id,
        TO_TIMESTAMP(hs.value_timestamp::INT) AT TIME ZONE c.exchange_timezone AS "timestamp",
        hs.value_open,
        hs.value_high,
        hs.value_low,
        hs.value_close,
        hs.volume
    FROM raw.FUTURES_HISTORY hs
    JOIN staging.future_info c ON c.symbol = hs.symbol
);