DROP TABLE IF EXISTS crypto_value;

CREATE TABLE crypto_value AS (
    SELECT
        'crypto_' || cv.symbol || '_value' AS crypto_id,
        cv.value_timestamp AS "timestamp",
        cv.value_open,
        cv.value_high,
        cv.value_low,
        cv.value_close,
        cv.volume
    FROM CRYPTO_HISTORY cv
);