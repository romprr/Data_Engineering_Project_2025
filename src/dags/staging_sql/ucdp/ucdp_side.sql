DROP TABLE IF EXISTS ucdp_side;

CREATE TABLE ucdp_side AS (
    SELECT DISTINCT
        c.conflict_id || '_' || c.start_date2 as episode_id,
        TRIM(UNNEST(regexp_split_to_array(c.gwno_a, ','))) as actor_id,
        'A' as side_type,
        c.year as "year"
    FROM 
        CONFLICT c
    WHERE c.gwno_a IS NOT NULL AND c.gwno_a != ''


    UNION

    SELECT DISTINCT
        c.conflict_id || '_' || c.start_date2 as episode_id,
        TRIM(UNNEST(regexp_split_to_array(c.gwno_b, ','))) as actor_id,
        'B' as side_type,
        c.year as "year"
    FROM 
        CONFLICT c
    WHERE c.gwno_b IS NOT NULL AND c.gwno_b != ''

    UNION

    SELECT DISTINCT
        c.conflict_id || '_' || c.start_date2 as episode_id,
        TRIM(UNNEST(regexp_split_to_array(c.gwno_a_2nd, ','))) as actor_id,
        'A_2ND' as side_type,
        c.year as "year"
    FROM 
        CONFLICT c
    WHERE c.gwno_a_2nd IS NOT NULL AND c.gwno_a_2nd != ''

    UNION

    SELECT DISTINCT
        c.conflict_id || '_' || c.start_date2 as episode_id,
        TRIM(UNNEST(regexp_split_to_array(c.gwno_b_2nd, ','))) as actor_id,
        'B_2ND' as side_type,
        c.year as "year"
    FROM 
        CONFLICT c
    WHERE c.gwno_b_2nd IS NOT NULL AND c.gwno_b_2nd != ''
);