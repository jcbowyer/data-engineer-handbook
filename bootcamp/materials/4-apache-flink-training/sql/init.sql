-- Create processed_events table
CREATE TABLE IF NOT EXISTS processed_events (
    ip VARCHAR,
    event_timestamp TIMESTAMP(3),
    referrer VARCHAR,
    host VARCHAR,
    url VARCHAR,
    geodata VARCHAR
);


CREATE TABLE IF NOT EXISTS processed_events_aggregated (
        event_hour TIMESTAMP(3),
        host VARCHAR,
        num_hits BIGINT
    );
    

 CREATE TABLE IF NOT EXISTS  session_metrics (
            session_start TIMESTAMP(3),
            session_end TIMESTAMP(3),
            ip VARCHAR,
            host VARCHAR,
            event_count BIGINT
        ) ;
