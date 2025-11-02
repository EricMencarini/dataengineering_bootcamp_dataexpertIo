/*
6- The incremental query to generate `host_activity_datelist`
*/
INSERT INTO user_devices_cumulated 

WITH last_snapshot AS (
    SELECT *
    FROM user_devices_cumulated
    ORDER BY date DESC
    LIMIT 1
),
next_day AS (
    SELECT
        e.user_id::text AS user_id,
        e.device_id::text AS device_id,
        d.browser_type AS browser_type,
        e.event_time::date AS date_active
    FROM events e
    JOIN devices d ON d.device_id = e.device_id
    WHERE e.event_time::date = (
        SELECT date + INTERVAL '1 day' FROM last_snapshot
    )
    GROUP BY 1,2,3,4
)

SELECT
    COALESCE(n.user_id, l.user_id) AS user_id,
    COALESCE(n.device_id, l.device_id) AS device_id,
    COALESCE(n.browser_type, l.browser_type) AS browser_type,
    CASE
        WHEN l.device_activity_datelist IS NULL THEN ARRAY[n.date_active]
        WHEN n.date_active IS NULL THEN l.device_activity_datelist
        ELSE ARRAY[n.date_active] || l.device_activity_datelist
    END AS device_activity_datelist,
    COALESCE(n.date_active, l.date + INTERVAL '1 day') AS date
FROM next_day n
FULL JOIN last_snapshot l
    ON n.user_id = l.user_id
   AND n.device_id = l.device_id
   AND n.browser_type = l.browser_type;
