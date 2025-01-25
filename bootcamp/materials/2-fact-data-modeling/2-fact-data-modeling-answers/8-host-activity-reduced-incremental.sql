 
WITH yesterday AS (
    SELECT * FROM host_activity_reduced 
    WHERE month = (current_date - interval '1 day')::date
),
today_metrics AS (
    SELECT 
        host,
        CAST(event_time AS timestamp)::date AS month,
        array_agg(daily_hits ORDER BY event_time) AS hits,
        array_agg(daily_uniques ORDER BY event_time) AS visitors
    FROM (
        SELECT 
            host,
            CAST(event_time AS timestamp)::date AS event_time,
            COUNT(*) AS daily_hits,
            COUNT(DISTINCT user_id) AS daily_uniques
        FROM events
        WHERE CAST(event_time AS timestamp)::date = current_date
        GROUP BY host, CAST(event_time AS timestamp)::date
    ) daily_stats
    GROUP BY host, CAST(event_time AS timestamp)::date
)
INSERT INTO host_activity_reduced
SELECT 
    COALESCE(t.month, y.month + interval '1 month'),
    COALESCE(t.host, y.host),
    COALESCE(
        y.hit_array || t.hits,
        y.hit_array,
        t.hits,
        ARRAY[]::INTEGER[]
    ) AS hit_array,
    COALESCE(
        y.unique_visitors_array || t.visitors,
        y.unique_visitors_array,
        t.visitors,
        ARRAY[]::INTEGER[]
    ) AS unique_visitors_array
FROM yesterday y
FULL OUTER JOIN today_metrics t 
    ON t.host = y.host;