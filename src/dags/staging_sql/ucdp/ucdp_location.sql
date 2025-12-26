DROP TABLE IF EXISTS ucdp_location;

CREATE TABLE ucdp_location AS (
    SELECT DISTINCT
        UNNEST(STRING_TO_ARRAY(c.location, ',')) AS "location"
    FROM CONFLICT c
);
