DROP TABLE IF EXISTS  staging.forex_info;

CREATE TABLE staging.forex_info AS (
    SELECT
        'forex_' || f.symbol || '_asset' AS forex_id,
        f.symbol as symbol,
        f.forex_name as forex_name,
        SPLIT_PART(f.forex_name, '/', 1) as from_currency,
        SPLIT_PART(f.forex_name, '/', 2) as to_currency,
        f.region as region,
        f.exchange_timezone as exchange_timezone
    FROM raw.FOREX_EXCHANGE f
);