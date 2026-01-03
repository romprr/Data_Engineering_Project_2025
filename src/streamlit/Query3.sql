SELECT 
    s.max_conflict_role,
	dai.name,
	dai.asset_type,
    AVG(
		CASE
			WHEN dai.asset_type = 'forex' AND dai.asset_from_currency = dai.asset_to_currency
				THEN 0
			WHEN dai.asset_type = 'forex' AND dai.asset_from_currency != 'USD'
				THEN (fav.close / fav.open) / 1
			ELSE (fav.close - fav.open) / fav.open
		END
	) AS monthly_returns
FROM production.fact_asset_value fav
JOIN production.dim_asset_info dai ON dai.asset_id = fav.asset_id
JOIN production.fact_monthly_country_status s 
    ON fav.date_id = s.month_id
WHERE s.actor_name LIKE 'Government of United States%'
	AND (dai.asset_type = 'forex' AND dai.name IN (
        'USD/JPY',
        'GBP/USD',
        'AUD/USD',
        'EUR/USD',
        'USD/CNY',
        'USD/RUB',
        'USD/MXN'
    ))
	OR (dai.asset_type = 'index' AND dai.name IN (
		'S&P 500',
		'Dow Jones Industrial Average',
		'NASDAQ Composite',
		'CBOE Volatility Index',
		'Russell 2000 Index',
		'NYSE Composite Index'		
	))
GROUP BY 3, 2, 1;