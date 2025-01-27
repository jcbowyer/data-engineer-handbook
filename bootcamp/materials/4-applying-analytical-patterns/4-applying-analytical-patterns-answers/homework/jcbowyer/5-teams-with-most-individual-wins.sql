/*
Teams with Most Total Wins
================================
This query analyzes team success by looking at games with positive plus/minus,
which indicates wins. It uses GROUPING SETS to provide a comprehensive view of team performance over the entire dataset period while excluding the "All Teams" aggregate row.

Output includes:
- Team abbreviation
- Total games played
- Number of wins (positive plus/minus games)
- Win percentage
- Date range of the analyzed period
*/

WITH game_seasons AS (
    -- Assign each game to the correct NBA season
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
team_performance AS (
    -- Calculate performance metrics for each team
    SELECT
        team_id,
        team_abbreviation,
        COUNT(DISTINCT game_id) AS games_played,
        COUNT(DISTINCT CASE WHEN plus_minus > 0 THEN game_id END) AS games_won,
        MIN(g.game_date_est) AS first_game,
        MAX(g.game_date_est) AS last_game
    FROM game_seasons g
    GROUP BY team_id, team_abbreviation
    HAVING COUNT(DISTINCT game_id) >= 82  -- Minimum full season of games
)
SELECT 
    team_abbreviation,
    SUM(games_played) AS games_played,
    SUM(games_won) AS games_won,
    ROUND(CAST(100.0 * SUM(games_won) / NULLIF(SUM(games_played), 0) AS numeric), 1) AS win_percentage,
    MIN(first_game) AS first_game,
    MAX(last_game) AS last_game
FROM team_performance
GROUP BY GROUPING SETS (
    (team_abbreviation) -- Team-specific rows only
)
HAVING GROUPING(team_abbreviation) = 0 -- Exclude aggregated "All Teams" row
ORDER BY games_won DESC NULLS LAST;
