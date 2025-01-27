SELECT 
    host,
    AVG(event_count) as avg_events_per_session,
    COUNT(*) as total_sessions
FROM session_metrics
GROUP BY host
ORDER BY avg_events_per_session DESC;