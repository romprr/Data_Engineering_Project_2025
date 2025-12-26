DROP TABLE IF EXISTS ucdp_region;

CREATE TABLE ucdp_region AS (
    SELECT DISTINCT
        TRIM(region)::INT as region_id,
        CASE TRIM(region) 
            WHEN '1' THEN 'Europe'
            WHEN '2' THEN 'Middle East'
            WHEN '3' THEN 'Asia'
            WHEN '4' THEN 'Africa'
            WHEN '5' THEN 'Americas'
        END as region_name,
        
        -- Minimum id in GWNO
        CASE TRIM(region) 
            WHEN '1' THEN 200
            WHEN '2' THEN 630
            WHEN '3' THEN 700
            WHEN '4' THEN 400
            WHEN '5' THEN 2
        END as min_gwno,

        -- Maximum id in GWNO
        CASE TRIM(region) 
            WHEN '1' THEN 399
            WHEN '2' THEN 699
            WHEN '3' THEN 999
            WHEN '4' THEN 626
            WHEN '5' THEN 199
        END as max_gwno
    FROM (
        SELECT DISTINCT
            UNNEST(STRING_TO_ARRAY(c.region, ',')) AS "region"
        FROM CONFLICT c
    )
);
