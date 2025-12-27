DROP TABLE IF EXISTS  staging.ucdp_conflict_location;

CREATE TABLE staging.ucdp_conflict_location AS (
    SELECT DISTINCT
        conflict_id as conflict_id,
        TRIM(location_split) AS location_id
    FROM raw.CONFLICT,
    LATERAL regexp_split_to_table(location, ',') AS location_split
    ORDER BY conflict_id, TRIM(location_split)
)