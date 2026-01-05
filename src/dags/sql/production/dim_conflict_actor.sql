DROP TABLE IF EXISTS production.dim_conflict_actor;

CREATE TABLE production.dim_conflict_actor AS(
    SELECT 
        actor_id,
        actor_name,
        actor_og_name
    FROM staging.ucdp_actors
)