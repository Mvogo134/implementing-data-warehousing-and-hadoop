-- ============================================================================
-- transform_activity.pig
-- Pig Latin script: clean and transform raw website activity.
--
-- Demonstrates the five core Pig operations:
--   LOAD   – read the CSV into a relation
--   FILTER – remove invalid / header rows
--   FOREACH – project useful fields
--   GROUP  – aggregate activity by product
--   STORE  – write the result back to HDFS
--
-- Run:
--   pig -x local pig/transform_activity.pig
--   (or inside a container with HDFS in the background)
-- ============================================================================

-- 1. LOAD raw data (no header skip; PigStorage handles CSV natively)
raw = LOAD 'hdfs:///user/hadoop/raw/website_activity.csv'
      USING PigStorage(',')
      AS (
          ts:chararray, customer_id:chararray, product_id:chararray,
          event_type:chararray, session_id:chararray, device:chararray,
          location:chararray, order_id:chararray
      );

-- 2. FILTER: remove the header row (has 'timestamp' in the first field)
valid = FILTER raw BY ts != 'timestamp';

-- 3. FILTER: keep only VIEW events (what we want to count)
views_only = FILTER valid BY event_type == 'VIEW';

-- 4. FOREACH: project product_id and clean empty product_ids (null rows)
clean = FOREACH views_only GENERATE
        TRIM(product_id) AS product_id;

-- 5. GROUP by product_id and count
grouped   = GROUP clean BY product_id;
view_counts = FOREACH grouped GENERATE
             group AS product_id,
             COUNT(clean) AS view_count;

-- 6. SORT for readability
sorted  = ORDER view_counts BY view_count DESC;

-- 7. STORE to HDFS (Pig will create part files)
STORE sorted INTO 'hdfs:///user/hadoop/output/pig_product_views'
     USING PigStorage('\t');