DROP TABLE IF EXISTS  staging.future_info;

CREATE TABLE staging.future_info AS (
    SELECT
        'future_' || fe.symbol || '_info' AS future_id,
        fe.symbol as symbol,
        REGEXP_REPLACE(fe.futures_name, '[, ]*[A-Za-z]{3}([- ]\d{2,4}|-)$', '', 'i') as futures_name,
        fe.region as region,
        fe.currency as currency,
        fe.exchange_timezone as exchange_timezone
    FROM raw.FUTURES_EXCHANGE fe
);