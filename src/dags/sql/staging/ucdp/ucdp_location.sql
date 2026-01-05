DROP TABLE IF EXISTS  staging.ucdp_location;

CREATE TABLE staging.ucdp_location AS (
    SELECT DISTINCT
        TRIM(UNNEST(STRING_TO_ARRAY(c.location, ','))) AS "location"
    FROM raw.CONFLICT c
);
