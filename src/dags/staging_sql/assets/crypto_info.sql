DROP TABLE IF EXISTS  staging.crypto_info;

CREATE TABLE staging.crypto_info AS (
    SELECT
        'crypto_' || c.symbol || '_info' AS crypto_id,
        c.symbol as symbol,
        split_part(c.symbol, '-', 1) AS from_currency,
        split_part(c.symbol, '-', 2) AS to_currency,
        c.crypto_name,
        c.region,
        c.exchange_timezone
    FROM raw.CRYPTO_EXCHANGE c
);