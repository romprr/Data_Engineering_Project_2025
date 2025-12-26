DROP TABLE IF EXISTS ucdp_conflict;

CREATE TABLE ucdp_conflict AS (
    SELECT DISTINCT
        c.conflict_id as conflict_id,
        c.incompatibility as reason,
        c.type_of_conflict as conflict_type,
        c.territory_name as disputed_territory,
        c.start_date as "start_date"
    FROM
        CONFLICT c
);