--Look at the grain of the table and verify if there is duplicates
SELECT
	game_id,team_id,player_id,count(1)
FROM
	game_details
GROUP BY 
	1,2,3
HAVING COUNT(1) > 1

--Looking at every duplication in the table and filtering to remove the duplicates
WITH deduped AS 
( 
  SELECT 
	*,
	ROW_NUMBER() OVER(PARTITION BY game_id, team_id,player_id) AS rownum
  FROM game_details
)

SELECT *
FROM deduped
WHERE rownum = 1
--ORDER BY rownum DESC

WITH deduped AS 
( 
  SELECT 
	g.game_date_est,
	gd.*,
	ROW_NUMBER() OVER(PARTITION BY gd.game_id, team_id,player_id ORDER BY g.game_date_est) AS rownum
  FROM game_details gd
  	JOIN games g on gd.game_id = g.game_id
)

SELECT *
FROM deduped
WHERE rownum = 1
--
CREATE TABLE fct_game_details(
	dim_game_date DATE,
	dim_season INTEGER,
	dim_team_id INTEGER,
	dim_player_id INTEGER,
	dim_player_name TEXT,
	dim_start_position TEXT,
	dim_is_playing_at_home BOOLEAN,
	dim_did_not_play BOOLEAN,
	dim_did_not_dress BOOLEAN,
	dim_not_with_team BOOLEAN,
	m_minutes REAL,
	m_fgm INTEGER,
	m_fga INTEGER,
	m_fg3m INTEGER,
	m_fg3a INTEGER,
	m_ftm INTEGER,
	m_fta INTEGER,
	m_oreb INTEGER,
	m_dreb INTEGER,
	m_reb INTEGER,
	m_ast INTEGER,
	m_stl INTEGER,
	m_blk INTEGER,
	m_turnovers INTEGER,
	m_pf INTEGER,
	m_pts INTEGER,
	m_plus_minus INTEGER,
	PRIMARY KEY(dim_game_date, dim_team_id, dim_player_id)
);
--
SELECT * FROM fct_game_details gd
JOIN teams t ON t.team_id = gd.dim_team_id

INSERT INTO fct_game_details
WITH deduped AS 
( 
  SELECT 
	g.game_date_est,
	g.season,
	g.home_team_id,
	gd.*,
	ROW_NUMBER() OVER(PARTITION BY gd.game_id, team_id, player_id ORDER BY g.game_date_est) AS rownum
  FROM game_details gd
  JOIN games g on gd.game_id = g.game_id
)
SELECT
	game_date_est AS dim_game_date,
	season AS dim_season,
	team_id AS dim_team_id,
	player_id AS dim_player_id,
	player_name AS dim_player_name,
	start_position AS dim_start_position,
	(team_id = home_team_id) AS dim_is_playing_at_home,
	COALESCE(POSITION('DNP' IN comment), 0) > 0 AS dim_did_not_play,
	COALESCE(POSITION('DND' IN comment), 0) > 0 AS dim_did_not_dress,
	COALESCE(POSITION('NWT' IN comment), 0) > 0 AS dim_not_with_team,
	(SPLIT_PART("min", ':', 1)::REAL + SPLIT_PART("min", ':', 2)::REAL / 60) AS m_minutes,
	fgm AS m_fgm,
	fga AS m_fga,
	fg3m AS m_fg3m,
	fg3a AS m_fg3a,
	ftm AS m_ftm,
	fta AS m_fta,
	oreb AS m_oreb,
	dreb AS m_dreb,
	reb AS m_reb,
	ast AS m_ast,
	stl AS m_stl,
	blk AS m_blk,
	"TO" AS m_turnovers,
	pf AS m_pf,
	pts AS m_pts,
	plus_minus AS m_plus_minus
FROM
	deduped
WHERE
	rownum = 1;

--
SELECT
	dim_player_name,
	dim_is_playing_at_home,
	COUNT(1) AS num_games,
	SUM(m_pts) AS total_points,
	COUNT(CASE WHEN dim_not_with_team THEN 1 END) AS bailed_num,
	CAST(COUNT(CASE WHEN dim_not_with_team THEN 1 END) AS REAL) / COUNT(1) AS bail_pct
FROM
	fct_game_details
GROUP BY
	1,2
ORDER BY
	6 DESC;

-----
-- SELECT 
-- 	MAX(event_time),
-- 	Min(event_time)
-- FROM 
-- 	events

-- CREATE TABLE users_cumulated(
-- 	user_id TEXT,
-- 	dates_active DATE[], --list of dates in the past where the user was active
-- 	date DATE, --current date for the user
-- 	PRIMARY KEY(user_id, date)
-- )
--INSERT INTO users_cumulated


WITH yesterday AS 
(
	SELECT *
	FROM users_cumulated
	WHERE date::date = '2023-01-30'
),
	today AS 
(
	SELECT
		user_id::text,
		--COUNT(1),
		event_time::date AS date_active	
	FROM 
		events
	WHERE 1=1
		AND event_time::date = DATE '2023-01-31'
		AND user_id IS NOT NULL
	GROUP BY
		user_id,
		event_time::date
)

SELECT
	COALESCE(t.user_id, y.user_id) AS user_id,
	CASE 
		WHEN y.dates_active IS NULL
			THEN ARRAY[t.date_active]
		WHEN t.date_active IS NULL THEN y.dates_active
			ELSE ARRAY[t.date_active] || y.dates_active
		END AS date_active,
	COALESCE(t.date_active, y.date + Interval  ' 1 day') AS date
FROM
	today t FULL JOIN yesterday y
	ON t.user_id = y.user_id

SELECT * FROM users_cumulated
WHERE date::date = '2023-01-02'


--Generate the series date of active days.
WITH users AS
(
	SELECT * FROM users_cumulated
	WHERE date::DATE = '2023-01-31'
),
	series AS (
		SELECT * 
		FROM generate_series('2023-01-01','2023-01-31', INTERVAL '1 Day') AS series_date
),
	place_older_ints AS
(
SELECT 
	(
		CASE
			WHEN dates_active @> ARRAY[series_date::date]
			THEN (POW(2, 32 - (date - series_date::date)))::bigint
			ELSE 0
		END
	)/*::bit(32)*/ AS placeholder_int_value,
	*
FROM
	users CROSS JOIN series
--WHERE user_id = '439578290726747300'
)

SELECT 
	user_id,
	SUM(placeholder_int_value)::bigint::bit(32) AS activity_bitmap,
	BIT_COUNT(SUM(placeholder_int_value)::bigint::bit(32)) > 0 AS dim_is_monthly_active,
	BIT_COUNT((SUM(placeholder_int_value)::bigint::bit(32) & B'11111110000000000000000000000000')) > 0 AS dim_is_weekly_active,
	BIT_COUNT((SUM(placeholder_int_value)::bigint::bit(32) & B'10000000000000000000000000000000')) > 0 AS dim_is_daily_active
FROM place_older_ints
GROUP BY user_id;