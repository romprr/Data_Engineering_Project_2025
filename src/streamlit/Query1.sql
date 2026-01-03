SELECT 
    r.region,
    cme.episode_id,
    ce.episode_start,
    da.asset_type,

    CASE 
        WHEN AVG(av.close) FILTER (WHERE m.month_date >= ce.episode_start) > 
             AVG(av.close) FILTER (WHERE m.month_date <  ce.episode_start) 
        THEN 'Growth'
        ELSE 'Loss'
    END as trend,

    -- 3 month (- and +) period Loss/Growth
    to_char(
        (
            (
                AVG(av.close) FILTER (WHERE m.month_date >= ce.episode_start) - 
                AVG(av.close) FILTER (WHERE m.month_date <  ce.episode_start)
            ) 
            / 
            AVG(av.close) FILTER (WHERE m.month_date < ce.episode_start)
        ) * 100,
        'FM9999990.99'
    ) || '%' as type_pct_change,

    -- Immediate shock of the markets
    to_char(
        AVG((av.close - av.open) / av.open) FILTER (WHERE m.month_date = DATE_TRUNC('month', ce.episode_start)) * 100,
        'FM9999990.99'
    ) || '%' as start_month_return,

    -- Panic metrics (volatility)
	STDDEV((av.close - av.open) / av.open) FILTER (WHERE m.month_date > ce.episode_start) * 100 as panic_score_after,
	STDDEV((av.close - av.open) / av.open) FILTER (WHERE m.month_date = DATE_TRUNC('month', ce.episode_start)) * 100 as panic_score_start,
	STDDEV((av.close - av.open) / av.open) FILTER (WHERE m.month_date <  ce.episode_start) * 100 as panic_score_before,

    -- All the assets linked to this specific analysis
    ARRAY_AGG(DISTINCT da.name) as assets_in_category

FROM production.fact_conflict_episode_monthly cme
JOIN production.dim_conflict_episode ce ON cme.episode_id = ce.episode_id
JOIN production.dim_month_date m 
    ON m.month_date BETWEEN DATE_TRUNC('month', ce.episode_start) - INTERVAL '3 months'
                        AND DATE_TRUNC('month', ce.episode_start) + INTERVAL '3 months'
JOIN production.dim_region r ON r.region_id = cme.region_id
JOIN production.fact_asset_value av ON m.month_id = av.date_id AND av.region_id = r.region_id
JOIN production.dim_asset_info da ON av.asset_id = da.asset_id AND da.asset_type != 'forex'

GROUP BY 
    r.region,
    cme.episode_id,
    ce.episode_start,
    da.asset_type

-- filter data for which we don't have months before
HAVING AVG(av.close) FILTER (WHERE m.month_date < ce.episode_start) > 0

ORDER BY 
    (
        (
            AVG(av.close) FILTER (WHERE m.month_date >= ce.episode_start) - 
            AVG(av.close) FILTER (WHERE m.month_date <  ce.episode_start)
        ) 
        / 
        AVG(av.close) FILTER (WHERE m.month_date < ce.episode_start)
    ) DESC;