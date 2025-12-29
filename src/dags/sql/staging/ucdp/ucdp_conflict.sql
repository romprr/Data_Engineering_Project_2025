DROP TABLE IF EXISTS  staging.ucdp_conflict;

CREATE TABLE staging.ucdp_conflict AS (
    SELECT DISTINCT
        c.conflict_id as conflict_id,
        c.incompatibility as reason,
        MAX(c.type_of_conflict) as conflict_type,
        c.territory_name as disputed_territory,
        c.start_date as "start_date"
    FROM
        raw.CONFLICT c
    GROUP BY c.conflict_id, c.incompatibility, c.territory_name, c.start_date
);