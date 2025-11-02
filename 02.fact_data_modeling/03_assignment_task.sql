/*
3- A cumulative query to generate `device_activity_datelist` from `events`
*/
INSERT INTO user_devices_cumulated
WITH yesterday AS 
(
	SELECT *
	FROM user_devices_cumulated
	WHERE date::date = '2023-01-03'
),
	today AS 
(
	SELECT
		e.user_id::text AS user_id,
		e.device_id::text AS device_id,
        d.browser_type AS browser_type,
        e.event_time::date AS date_active
	FROM 
		events e 
        JOIN devices d ON d.device_id = e.device_id
	WHERE 1=1
		AND event_time::date = '2023-01-04'
		AND e.user_id IS NOT NULL
        AND e.device_id IS NOT NULL
	GROUP BY
		e.user_id,
		e.device_id,
        d.browser_type,
        e.event_time::date
)

SELECT
	COALESCE(t.user_id, y.user_id) AS user_id,
    COALESCE(t.device_id, y.device_id) AS device_id,
    COALESCE(t.browser_type, y.browser_type) AS browser_type,
	CASE 
		WHEN y.device_activity_datelist IS NULL
			THEN ARRAY[t.date_active]
		WHEN t.date_active IS NULL 
            THEN y.device_activity_datelist
		ELSE ARRAY[t.date_active] || y.device_activity_datelist
		END AS device_activity_datelist,
	COALESCE(t.date_active, y.date + Interval  ' 1 day') AS date
FROM
	today t FULL JOIN yesterday y
	ON t.user_id = y.user_id AND t.device_id = y.device_id AND t.browser_type = y.browser_type