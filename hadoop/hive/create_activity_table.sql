
-- 1. CREATE DATABASE
CREATE DATABASE IF NOT EXISTS ecommerce_analytics;
USE ecommerce_analytics;

-- 2. CREATE TABLE  (managed table)
--    File already on HDFS. Load it into Hive's managed warehouse.
DROP TABLE IF EXISTS website_activity;
CREATE TABLE website_activity (
    ts           STRING,
    customer_id  STRING,
    product_id   STRING,
    event_type   STRING,
    session_id   STRING,
    device       STRING,
    location     STRING,
    order_id     STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

-- 3. LOAD DATA  (move into Hive warehouse)
--    Assumes the file is already in HDFS at /user/hadoop/raw/
LOAD DATA INPATH '/user/hadoop/raw/website_activity.csv' INTO TABLE website_activity;

-- 4. Verify the load
SELECT * FROM website_activity LIMIT 10;
