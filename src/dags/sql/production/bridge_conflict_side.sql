DROP TABLE IF EXISTS production.bridge_conflict_side;

CREATE TABLE production.bridge_conflict_side AS (
    SELECT * FROM staging.ucdp_side
);
