DROP TABLE IF EXISTS  staging.crypto_value;

CREATE TABLE staging.crypto_value AS (
    SELECT
        'crypto_' || hs.symbol || '_value' AS crypto_id,
        TO_TIMESTAMP(hs.value_timestamp::INT) AT TIME ZONE c.exchange_timezone AS "timestamp",
        hs.value_open,
        hs.value_high,
        hs.value_low,
        hs.value_close,
        hs.volume
    FROM raw.CRYPTO_HISTORY hs
    JOIN staging.crypto_info c ON c.symbol = hs.symbol
);