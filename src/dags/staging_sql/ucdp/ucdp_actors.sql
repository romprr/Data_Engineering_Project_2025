DROP TABLE IF EXISTS staging.ucdp_actors;

CREATE TABLE staging.ucdp_actors AS(
    SELECT DISTINCT
        actor_id as actor_id,
        actor_name as actor_name,
        actor_og_name as actor_og_name,
        STRING_TO_ARRAY(conflict_ids, ',')::INT[] as conflicts_participations
    FROM raw.UCDP_ACTORS
);