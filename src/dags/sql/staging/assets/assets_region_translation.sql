DROP TABLE IF EXISTS staging.assets_region_translation;

CREATE TABLE staging.assets_region_translation AS (
    with timezones as (
        SELECT DISTINCT exchange_timezone FROM staging.index_info
        UNION
        SELECT DISTINCT exchange_timezone FROM staging.forex_info
        UNION
        SELECT DISTINCT exchange_timezone FROM staging.future_info
        UNION
        SELECT DISTINCT exchange_timezone FROM staging.crypto_info
    )

    SELECT
        exchange_timezone,
        CASE 
            WHEN exchange_timezone LIKE '%/Istanbul' 
            OR exchange_timezone LIKE '%/Cairo'
            OR exchange_timezone LIKE '%/Dubai' 
            OR exchange_timezone LIKE '%/Riyadh' 
            OR exchange_timezone LIKE '%/Tehran' 
            OR exchange_timezone LIKE '%/Baghdad' 
            OR exchange_timezone LIKE '%/Kuwait' 
            OR exchange_timezone LIKE '%/Jerusalem' 
            OR exchange_timezone LIKE '%/Tel_Aviv' 
            OR exchange_timezone LIKE '%/Beirut' 
            OR exchange_timezone LIKE '%/Amman' 
            OR exchange_timezone LIKE '%/Bahrain' 
            OR exchange_timezone LIKE '%/Qatar' 
            OR exchange_timezone LIKE '%/Muscat' 
            OR exchange_timezone LIKE '%/Aden' 
            OR exchange_timezone LIKE '%/Damascus' 
            THEN 'Middle East'

        WHEN exchange_timezone LIKE 'Europe/%'
            OR exchange_timezone LIKE '%/Baku'
            OR exchange_timezone LIKE '%/Tbilisi'
            OR exchange_timezone LIKE '%/Yerevan'
            OR exchange_timezone LIKE '%/Nicosia'
            OR exchange_timezone LIKE '%/Famagusta'
            OR exchange_timezone IN (
                'Asia/Yekaterinburg', 'Asia/Omsk', 'Asia/Novosibirsk', 'Asia/Krasnoyarsk',
                'Asia/Irkutsk', 'Asia/Yakutsk', 'Asia/Vladivostok', 'Asia/Magadan',
                'Asia/Kamchatka', 'Asia/Sakhalin', 'Asia/Anadyr'
            )
            THEN 'Europe'

        WHEN exchange_timezone LIKE 'Africa/%' 
            THEN 'Africa'

        WHEN exchange_timezone LIKE 'America/%' 
            OR exchange_timezone LIKE 'US/%' 
            OR exchange_timezone LIKE 'Canada/%' 
            OR exchange_timezone LIKE 'Brazil/%' 
            OR exchange_timezone LIKE 'Mexico/%' 
            OR exchange_timezone LIKE 'Chile/%'
        THEN 'Americas'

        WHEN exchange_timezone LIKE 'Asia/%' 
            OR exchange_timezone LIKE 'Australia/%' 
            OR exchange_timezone LIKE 'Pacific/%'
            OR exchange_timezone LIKE 'Indian/%'
        THEN 'Asia'

        ELSE 'Unknown'
    END AS region
    
    FROM timezones
)