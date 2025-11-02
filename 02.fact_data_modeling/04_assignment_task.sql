/*
4- A `datelist_int` generation query. 
Convert the `device_activity_datelist` column into a `datelist_int` column 
*/
WITH user_devices AS (
    SELECT
        user_id,
        device_id,
        browser_type,
        device_activity_datelist,
        date AS snapshot_date
    FROM user_devices_cumulated
    WHERE date = (SELECT MAX(date) FROM user_devices_cumulated)
),
calendar AS (
    SELECT series_date::date
    FROM generate_series(
        (SELECT MIN(d) FROM (
            SELECT UNNEST(device_activity_datelist) AS d
            FROM user_devices
        ) s),
        (SELECT MAX(date) FROM user_devices_cumulated),
        INTERVAL '1 day'
    ) AS series_date
),
placeholder_ints AS (
    SELECT
        ud.user_id,
        ud.device_id,
        ud.browser_type,
        CASE
            WHEN ud.device_activity_datelist @> ARRAY[cal.series_date]::date[]
            THEN (1::bigint << (31 - (ud.snapshot_date - cal.series_date)))
            ELSE 0
        END AS placeholder_int_value
    FROM user_devices ud
    CROSS JOIN calendar cal
)
SELECT
    user_id,
    device_id,
    browser_type,
    CAST(SUM(placeholder_int_value) AS BIGINT) AS datelist_int,
    CAST(CAST(SUM(placeholder_int_value) AS BIGINT) AS BIT(32)) AS activity_bitmap,
    (CAST(SUM(placeholder_int_value) AS BIGINT) <> 0) AS dim_is_monthly_active,
    ((CAST(SUM(placeholder_int_value) AS BIGINT)) & 254) <> 0 AS dim_is_weekly_active
FROM placeholder_ints
GROUP BY user_id, device_id, browser_type;