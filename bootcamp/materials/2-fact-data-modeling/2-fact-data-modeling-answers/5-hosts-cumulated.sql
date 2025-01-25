DROP TABLE IF EXISTS hosts_cumulated CASCADE;

CREATE TABLE hosts_cumulated (
   host TEXT NOT NULL,
   host_activity_datelist DATE[] NOT NULL,
   PRIMARY KEY (host)
);

WITH daily_activity AS (
 SELECT 
   host,
   event_time::date as activity_date
 FROM events
 WHERE host IS NOT NULL 
 GROUP BY host, event_time::date
)
INSERT INTO hosts_cumulated
SELECT 
 host,
 array_agg(DISTINCT activity_date ORDER BY activity_date) as host_activity_datelist
FROM daily_activity
GROUP BY host
ON CONFLICT (host) DO UPDATE
SET host_activity_datelist = 
 array(SELECT DISTINCT unnest(array_cat(hosts_cumulated.host_activity_datelist, 
           EXCLUDED.host_activity_datelist)) ORDER BY 1);