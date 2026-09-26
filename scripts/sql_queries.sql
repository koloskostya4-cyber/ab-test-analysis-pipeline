DROP TABLE user_sessions;


-- Create a table
CREATE TABLE IF NOT exists user_sessions (
    user_id INTEGER PRIMARY KEY,
    group_type VARCHAR(1) CHECK (group_type IN ('A', 'B')),
    page_views INTEGER,
    time_spent INTEGER, -- in seconds
    conversion VARCHAR(3) CHECK (conversion IN ('Yes', 'No')),
    device VARCHAR(20),
    location VARCHAR(50)
);


-- Importing data from CSV
COPY user_sessions (user_id, group_type, page_views, time_spent, conversion, device, location)
FROM '/data/company/data.csv'
DELIMITER ','
CSV HEADER;

-- test
SELECT COUNT(*) FROM user_sessions;


-- 1 Comparison of average time spent on the site for users in groups A and B
-- with percentage differences highlighted (aggregation + self-join)
WITH group_stats AS (
    SELECT 
        group_type,
        AVG(time_spent) AS avg_time,
        COUNT(*) AS user_count
    FROM user_sessions
    GROUP BY group_type
)
SELECT 
    ROUND(a.avg_time, 2) AS avg_time_a,
    ROUND(b.avg_time, 2) AS avg_time_b,
    ROUND((b.avg_time - a.avg_time) / a.avg_time * 100, 2) AS pct_change
FROM group_stats a
JOIN group_stats b ON a.group_type = 'A' AND b.group_type = 'B';

-- 2 Top-3 users by time on the site in each group
SELECT 
    user_id,
    group_type,
    time_spent,
    rank_in_group
FROM (
    SELECT 
        user_id,
        group_type,
        time_spent,
        DENSE_RANK() OVER (PARTITION BY group_type ORDER BY time_spent DESC) AS rank_in_group
    FROM user_sessions
) AS ranked_users
WHERE rank_in_group <= 3
ORDER BY group_type ASC, rank_in_group ASC;

-- 3 Difference in time spent on the site between adjacent users in the same group, in descending order
SELECT 
    user_id,
    group_type,
    time_spent,
    LAG(time_spent, 1) OVER (PARTITION BY group_type ORDER BY time_spent DESC) AS prev_time,
    time_spent - LAG(time_spent, 1) OVER (PARTITION BY group_type ORDER BY time_spent DESC) AS time_diff
FROM user_sessions
ORDER BY time_spent DESC;

-- 4 Moving average Page Views (5 users) for group A.
SELECT 
    user_id,
    page_views,
    ROUND(AVG(page_views) OVER (ORDER BY user_id ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING), 2) AS moving_avg_5
FROM user_sessions
WHERE group_type = 'A';



-- 5 Atomic update: If a user from group B spends >120 seconds,
-- mark them as "High Engagement" in the new table

BEGIN; -- Start of transaction

-- Create a table for labels
CREATE TABLE IF NOT EXISTS user_engagement (
    user_id INTEGER PRIMARY KEY,
    engagement_level VARCHAR(20)
);

-- Inserting or updating labels
INSERT INTO user_engagement (user_id, engagement_level)
SELECT 
    user_id,
    CASE 
        WHEN time_spent > 120 THEN 'High'
        WHEN time_spent BETWEEN 60 AND 120 THEN 'Medium'
        ELSE 'Low'
    END
FROM user_sessions
WHERE group_type = 'B'
ON CONFLICT (user_id) 
DO UPDATE SET engagement_level = EXCLUDED.engagement_level;

-- Checking Consistency (ACID: Consistency)
SELECT COUNT(*) FROM user_engagement 
WHERE user_id IN (SELECT user_id FROM user_sessions WHERE group_type = 'B');

COMMIT;
