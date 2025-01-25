WITH RECURSIVE date_sequence AS (
 SELECT 
   (SELECT MIN(d)::date FROM user_devices_cumulated, unnest(device_activity_datelist) d) as valid_date
 UNION ALL
 SELECT (valid_date + interval '1 day')::date
 FROM date_sequence
 WHERE valid_date < (SELECT MAX(d)::date FROM user_devices_cumulated, unnest(device_activity_datelist) d)
),
activity_bits AS (
 SELECT 
   uc.user_id,
   uc.browser_type,
   uc.device_activity_datelist @> ARRAY[ds.valid_date::date] AS is_active,
   (
     SELECT MAX(d)::date - ds.valid_date
     FROM unnest(uc.device_activity_datelist) d
   ) AS days_since
 FROM user_devices_cumulated uc
 CROSS JOIN date_sequence ds
),
datelist_integers AS (
 SELECT 
   user_id,
   browser_type,
   SUM(
     CASE WHEN is_active 
     THEN POWER(2, 32 - (days_since::integer))::bigint 
     ELSE 0 END
   )::bigint AS datelist_int
 FROM activity_bits
 GROUP BY user_id, browser_type
)
SELECT * FROM datelist_integers;