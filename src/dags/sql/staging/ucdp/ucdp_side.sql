DROP TABLE IF EXISTS  staging.ucdp_side;

CREATE TABLE staging.ucdp_side AS (
    -- Block 1: Side A
    SELECT DISTINCT
        c.conflict_id || '_' || c.start_date2 as episode_id,
        act.actor_id as actor_id,  -- Getting the ID from the JOIN
        'A' as side_type,
        c.year as "year"
    FROM 
        raw.CONFLICT c
    -- Split the name string into rows (Lateral Join)
    CROSS JOIN LATERAL UNNEST(regexp_split_to_array(c.side_a, ',')) AS raw_name
    -- Join with Actors table on the trimmed name
    JOIN staging.UCDP_ACTORS act ON TRIM(raw_name) = act.actor_name AND c.conflict_id = ANY(conflicts_participations)
    WHERE c.side_a IS NOT NULL AND c.side_a != ''

    UNION

    -- Block 2: Side B
    SELECT DISTINCT
        c.conflict_id || '_' || c.start_date2 as episode_id,
        act.actor_id as actor_id,
        'B' as side_type,
        c.year as "year"
    FROM 
        raw.CONFLICT c
    CROSS JOIN LATERAL UNNEST(regexp_split_to_array(c.side_b, ',')) AS raw_name
    JOIN staging.UCDP_ACTORS act ON TRIM(raw_name) = act.actor_name AND c.conflict_id = ANY(conflicts_participations)
    WHERE c.side_b IS NOT NULL AND c.side_b != ''

    UNION

    -- Block 3: Side A 2nd
    SELECT DISTINCT
        c.conflict_id || '_' || c.start_date2 as episode_id,
        act.actor_id as actor_id,
        'A_2ND' as side_type,
        c.year as "year"
    FROM 
        raw.CONFLICT c
    CROSS JOIN LATERAL UNNEST(regexp_split_to_array(c.side_a_2nd, ',')) AS raw_name
    JOIN staging.UCDP_ACTORS act ON TRIM(raw_name) = act.actor_name AND c.conflict_id = ANY(conflicts_participations)
    WHERE c.side_a_2nd IS NOT NULL AND c.side_a_2nd != ''

    UNION

    -- Block 4: Side B 2nd
    SELECT DISTINCT
        c.conflict_id || '_' || c.start_date2 as episode_id,
        act.actor_id as actor_id,
        'B_2ND' as side_type,
        c.year as "year"
    FROM 
        raw.CONFLICT c
    CROSS JOIN LATERAL UNNEST(regexp_split_to_array(c.side_b_2nd, ',')) AS raw_name
    JOIN staging.UCDP_ACTORS act ON TRIM(raw_name) = act.actor_name AND c.conflict_id = ANY(conflicts_participations)
    WHERE c.side_b_2nd IS NOT NULL AND c.side_b_2nd != ''
);