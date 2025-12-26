DROP TABLE IF EXISTS forex_info;

CREATE TABLE forex_info AS (
    SELECT
        'forex_' || f.symbol || '_info' AS forex_id,
        f.symbol as symbol,
        f.forex_name as forex_name,
        SPLIT_PART(f.forex_name, '/', 1) as from_currency,
        SPLIT_PART(f.forex_name, '/', 2) as to_currency,
        f.region as region,
        f.exchange_timezone as exchange_timezone
    FROM
        FOREX_EXCHANGE f
);