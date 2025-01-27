/*
Player Most Points for a Single Team (Using GROUPING SETS)
========================================================
This query finds the players who scored the most total points while playing
for a specific team using the GROUPING SETS approach. It leverages the base
aggregation query to extract player-team career statistics.

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
    -- First determine the season for each game
    SELECT 
        gd.*,
        games.game_date_est,
        EXTRACT(YEAR FROM CASE 
            WHEN EXTRACT(MONTH FROM games.game_date_est) >= 10 THEN 
                games.game_date_est + INTERVAL '3 months'
            ELSE 
                games.game_date_est - INTERVAL '9 months'
        END) AS season
    FROM game_details gd
    JOIN games ON gd.game_id = games.game_id
),
aggregated_stats AS (
    -- Use GROUPING SETS to efficiently compute multiple aggregation levels
    SELECT
        player_id,
        player_name,
        team_id,
        team_abbreviation,
        season,
        -- Basic counting stats
        COUNT(DISTINCT game_id) as games_played,
        SUM(pts) as total_points,
        ROUND(CAST(AVG(pts) AS numeric), 2) as avg_points,
        MIN(game_date_est) as start_date,
        MAX(game_date_est) as end_date
    FROM game_seasons
    GROUP BY GROUPING SETS (
        (player_id, player_name, team_id, team_abbreviation), -- Player-Team combination stats
        (player_id, player_name, season),                     -- Player stats by season
        (team_id, team_abbreviation)                          -- Overall team stats
    )
)
-- Get the most prolific player-team combinations
SELECT 
    player_name,
    team_abbreviation,
    total_points,
    games_played,
    ROUND(CAST(total_points AS numeric) / CAST(games_played AS numeric), 1) as ppg,
    start_date as tenure_start,
    end_date as tenure_end
FROM aggregated_stats
WHERE 
    player_id IS NOT NULL 
    AND team_id IS NOT NULL 
    AND season IS NULL -- Select only Player-Team level stats
    AND games_played >= 20  -- Minimum games threshold
ORDER BY total_points DESC
LIMIT 10;