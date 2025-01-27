/*
Lebron 10 Game Streak Analysis Query
================================

Homework Question: How many games in a row did LeBron James score over 10 points a game?

This query analyzes LeBron James' scoring streaks to find his longest streak of
consecutive games scoring more than 10 points. It identifies streak boundaries by
looking for games where he scored 10 or fewer points or gaps in his game appearances.

Key Features:
- Tracks consecutive games with > 10 points
- Identifies start and end dates of streaks
- Handles season boundaries appropriately
- Includes game count for each streak

Implementation Details:
1. lebron_games: Gets all LeBron's games with points scored
2. streak_boundaries: Identifies where streaks start/end
3. streak_groups: Groups consecutive games into streaks
4. streak_stats: Calculates stats for each streak

Output Columns:
- streak_games: Number of consecutive games in streak
- points_per_game: Average points during streak
- streak_start: Date of first game in streak
- streak_end: Date of last game in streak
- days_in_streak: Duration of streak in days
*/

WITH lebron_games AS (
    -- Get all of LeBron's games and points scored
    SELECT 
        g.game_date_est,
        gd.pts,
        -- Calculate days since last game to identify gaps
        g.game_date_est - LAG(g.game_date_est) OVER (
            ORDER BY g.game_date_est
        ) as days_since_last_game
    FROM game_details gd
    JOIN games g ON gd.game_id = g.game_id
    WHERE gd.player_name = 'LeBron James'
    ORDER BY g.game_date_est
),
streak_boundaries AS (
    -- Identify start of new streaks
    SELECT 
        game_date_est,
        pts,
        CASE
            -- Start new streak if:
            WHEN LAG(pts) OVER (ORDER BY game_date_est) <= 10 
                OR LAG(pts) OVER (ORDER BY game_date_est) IS NULL
                OR days_since_last_game > 30  -- Gap in games
            THEN 1
            ELSE 0
        END as new_streak
    FROM lebron_games
    WHERE pts > 10  -- Only include games with > 10 points
),
streak_groups AS (
    -- Group consecutive games into streaks
    SELECT 
        game_date_est,
        pts,
        SUM(new_streak) OVER (ORDER BY game_date_est) as streak_id
    FROM streak_boundaries
),
streak_stats AS (
    -- Calculate statistics for each streak
    SELECT 
        streak_id,
        COUNT(*) as streak_games,
        ROUND(CAST(AVG(pts) AS numeric), 1) as points_per_game,
        MIN(game_date_est) as streak_start,
        MAX(game_date_est) as streak_end,
        MAX(game_date_est) - MIN(game_date_est) as days_in_streak
    FROM streak_groups
    GROUP BY streak_id
)
-- Show longest streak
SELECT *
FROM streak_stats
WHERE streak_games = (SELECT MAX(streak_games) FROM streak_stats)
ORDER BY streak_start;