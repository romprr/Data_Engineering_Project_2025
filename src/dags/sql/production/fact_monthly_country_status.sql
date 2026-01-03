DROP TABLE IF EXISTS production.fact_monthly_country_status;

CREATE TABLE production.fact_monthly_country_status AS (
    WITH relevant_actors AS (
        SELECT DISTINCT actor_id 
        FROM production.bridge_conflict_side
    ),
    
    actor_timeline AS (
        SELECT 
            ra.actor_id,
            d.month_id
        FROM relevant_actors ra
        CROSS JOIN production.dim_month_date d
    ),
    
    conflict_activity AS (
        SELECT
            fcem.month_id,
            bcs.actor_id,
            bcs.episode_id,
            CASE 
                WHEN bcs.side IN ('A', 'B') THEN 2 -- Primary
                WHEN bcs.side IN ('A_2ND', 'B_2ND') THEN 1 -- Support
                ELSE 0
            END as role_weight
        FROM production.bridge_conflict_side bcs
        JOIN production.fact_conflict_episode_monthly fcem 
            ON bcs.episode_id = fcem.episode_id
    )

    SELECT
        at.month_id,
        at.actor_id,
        dca.actor_name,
        
        COUNT(DISTINCT ca.episode_id) as active_conflict_count,
        
        CASE 
            WHEN MAX(ca.role_weight) = 2 THEN 'Primary Party'
            WHEN MAX(ca.role_weight) = 1 THEN 'Support Role'
            ELSE 'Peace'
        END as max_conflict_role

    FROM actor_timeline at
    JOIN production.dim_conflict_actor dca ON at.actor_id = dca.actor_id
    LEFT JOIN conflict_activity ca ON at.actor_id = ca.actor_id AND at.month_id = ca.month_id
    GROUP BY at.month_id, at.actor_id, dca.actor_name
);