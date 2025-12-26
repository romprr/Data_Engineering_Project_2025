DROP TABLE IF EXISTS future_info;

CREATE TABLE future_info AS (
    SELECT
        'future_' || fe.symbol || '_info' AS future_id,
        fe.symbol as symbol,
        fe.futures_name as futures_name,
        fe.region as region,
        fe.currency as currency,
        fe.exchange_timezone as exchange_timezone
    FROM
        FUTURES_EXCHANGE fe
);