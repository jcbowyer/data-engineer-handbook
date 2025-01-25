Truncate user_devices_cumulated;
  
WITH daily_activity AS (
 SELECT 
   user_id,
   CASE 
     WHEN lower(d.browser_type) LIKE '%bot%' THEN 'Bot'
     WHEN d.browser_type = 'Chrome Mobile' THEN 'Chrome' 
     WHEN d.browser_type = 'Other' THEN 'Other'
     ELSE d.browser_type
   END as browser_type,
   event_time::date as activity_date
 FROM events e
 JOIN devices d ON e.device_id = d.device_id
 WHERE e.user_id IS NOT NULL
 GROUP BY user_id, d.browser_type, event_time::date
),
aggregated_activity AS (
 SELECT
   user_id,
   browser_type,
   array_agg(DISTINCT activity_date ORDER BY activity_date) as device_activity_datelist
 FROM daily_activity
 GROUP BY user_id, browser_type
)
INSERT INTO user_devices_cumulated
SELECT * FROM aggregated_activity 
ON CONFLICT (user_id, browser_type) DO UPDATE
SET device_activity_datelist = 
 array(SELECT DISTINCT unnest(array_cat(user_devices_cumulated.device_activity_datelist, 
           EXCLUDED.device_activity_datelist)) ORDER BY 1);