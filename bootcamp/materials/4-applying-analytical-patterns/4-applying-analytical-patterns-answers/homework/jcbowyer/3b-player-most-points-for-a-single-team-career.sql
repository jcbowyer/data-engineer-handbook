/*
Player Most Points for a Single Team (Using GROUPING SETS)
========================================================
This query finds the players who scored the most total points while playing
for a specific team using the GROUPING SETS approach. It leverages the base
aggregation query to extract player-team career statistics across all seasons.

Output includes:
- Total points scored with the team
- Games played to provide context
- Average points per game
- Career span with the team

Implementation Notes:
- Uses GROUPING SETS for efficient aggregation
- Requires minimum 20 games with a team
- Filters for Player-Team Stats specifically
- Orders by total points to find highest scorers
*/

WITH game_seasons AS (
    -- Assign games to the correct NBA season
    SELECT 
        gd.*,
        g.game_date_est,
        EXTRACT(YEAR FROM CASE 
            WHEN EXTRACT(MONTH FROM g.game_date_est) >= 10 THEN 
                g.game_date_est + INTERVAL '3 months'
            ELSE 
                g.game_date_est - INTERVAL '9 months'
        END) AS season
    FROM game_details gd
    JOIN games g ON gd.game_id = g.game_id
),
aggregated_stats AS (
    -- Aggregate statistics using GROUPING SETS
    SELECT
        player_id,
        player_name,
        team_id,
        team_abbreviation,
        NULL AS season, -- NULL season to group across all seasons
        COUNT(DISTINCT game_id) AS games_played,
        SUM(pts) AS total_points,
        ROUND(CAST(SUM(pts) AS numeric) / NULLIF(COUNT(DISTINCT game_id), 0), 1) AS avg_points,
        MIN(game_date_est) AS start_date,
        MAX(game_date_est) AS end_date
    FROM game_seasons
    GROUP BY GROUPING SETS (
        (player_id, player_name, team_id, team_abbreviation) -- Player-Team combination stats
    )
)
-- Select top player-team combinations by total points
SELECT 
    player_name,
    team_abbreviation,
    total_points,
    games_played,
    ROUND(CAST(total_points AS numeric) / CAST(games_played AS numeric), 1) AS ppg,
    start_date AS tenure_start,
    end_date AS tenure_end
FROM aggregated_stats
WHERE 
    player_id IS NOT NULL 
    AND team_id IS NOT NULL 
    AND games_played >= 20  -- Minimum games threshold
ORDER BY total_points DESC
LIMIT 10;
