DROP TABLE IF EXISTS crypto_info;

CREATE TABLE crypto_info AS (
    SELECT
        'crypto_' || c.symbol || '_info' AS crypto_id,
        split_part(c.symbol, '-', 1) AS from_currency,
        split_part(c.symbol, '-', 2) AS to_currency,
        c.crypto_name,
        c.region,
        c.exchange_timezone
    FROM CRYPTO_EXCHANGE c
);