WITH daily_activity AS (
 SELECT host, event_time::date as activity_date
 FROM events e1
 WHERE host IS NOT NULL 
 AND NOT EXISTS (
   SELECT 1 FROM hosts_cumulated hc
   WHERE hc.host = e1.host
   AND hc.host_activity_datelist @> ARRAY[e1.event_time::date]
 )
 GROUP BY host, event_time::date  
)
INSERT INTO hosts_cumulated
SELECT 
 host,
 array_agg(activity_date ORDER BY activity_date) as host_activity_datelist
FROM daily_activity
GROUP BY host
ON CONFLICT (host) DO UPDATE
SET host_activity_datelist = 
 array(SELECT DISTINCT unnest(array_cat(hosts_cumulated.host_activity_datelist, 
           EXCLUDED.host_activity_datelist)) ORDER BY 1);