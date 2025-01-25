-- Create the table if it does not exist
CREATE TABLE IF NOT EXISTS host_activity_reduced (
   month DATE NOT NULL,
   host TEXT NOT NULL,
   hit_array INTEGER[] NOT NULL,
   unique_visitors_array INTEGER[] NOT NULL,
   PRIMARY KEY (month, host)
);

truncate host_activity_reduced;

-- Insert data into the table
INSERT INTO host_activity_reduced
SELECT 
   date_trunc('month', event_time)::date as month, -- Corrected usage of date_trunc
   host,
   array_agg(hits ORDER BY day) as hit_array,
   array_agg(visitors ORDER BY day) as unique_visitors_array
FROM (
   SELECT 
       date_trunc('month', event_time::timestamp) as event_time, -- Cast event_time to timestamp
       host,
       EXTRACT(DAY FROM event_time::timestamp)::integer as day, -- Cast event_time to timestamp
       COUNT(*) as hits,
       COUNT(DISTINCT user_id) as visitors
   FROM events 
   WHERE host IS NOT NULL
   GROUP BY 1, 2, 3
) daily_stats
GROUP BY 1, 2;
