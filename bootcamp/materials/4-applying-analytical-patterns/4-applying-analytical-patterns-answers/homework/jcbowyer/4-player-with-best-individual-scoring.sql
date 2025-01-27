/*
Players with Best Individual Scoring Seasons
================================================
This query identifies the highest scoring individual seasons in the dataset.
It calculates total points and per-game averages for each player-season
combination using GROUPING SETS to enable flexible aggregation.

Output includes:
- Player name
- Season year
- Total points scored
- Games played in the season
- Average points per game

Implementation Notes:
- Uses game_details table for player stats
- Calculates NBA seasons based on game dates
- Requires minimum 20 games in a season
- Sorts by total points scored to find highest scoring seasons
*/

WITH game_seasons AS (
    -- Determine the season for each game
    SELECT 
        g.game_id,
        g.game_date_est,
        gd.player_id,
        gd.player_name,
        gd.pts,
        EXTRACT(YEAR FROM CASE 
            WHEN EXTRACT(MONTH FROM g.game_date_est) >= 10 THEN 
                g.game_date_est + INTERVAL '3 months'
            ELSE 
                g.game_date_est - INTERVAL '9 months'
        END) AS season
    FROM game_details gd
    JOIN games g ON gd.game_id = g.game_id
),
season_stats AS (
    -- Calculate season totals and averages for each player
    SELECT
        player_id,
        player_name,
        season,
        COUNT(DISTINCT game_id) AS games_played,
        SUM(pts) AS total_points,
        ROUND(CAST(SUM(pts) AS NUMERIC) / CAST(COUNT(DISTINCT game_id) AS NUMERIC), 1) AS avg_ppg
    FROM game_seasons
    GROUP BY player_id, player_name, season
    HAVING COUNT(DISTINCT game_id) >= 20 -- Minimum games threshold
)
SELECT 
    CASE 
        WHEN GROUPING(player_name) = 0 AND GROUPING(season) = 0 THEN player_name
        ELSE NULL
    END AS player_name,
    CASE 
        WHEN GROUPING(season) = 0 THEN CAST(season AS VARCHAR)
        ELSE NULL
    END AS season,
    SUM(total_points) AS total_points,
    SUM(games_played) AS games_played,
    ROUND(SUM(total_points)::NUMERIC / NULLIF(SUM(games_played), 0), 1) AS avg_ppg
FROM season_stats
GROUP BY GROUPING SETS (
    (player_name, season) -- Individual player-season combinations
)
HAVING GROUPING(player_name) = 0 AND GROUPING(season) = 0 -- Exclude aggregate rows
ORDER BY total_points DESC
LIMIT 10;
