USE ecommerce_analytics;

-- Q1. Product views (MapReduce equivalent)
-- "How many times was each product viewed?"
SELECT product_id, COUNT(*) AS view_count
FROM website_activity
WHERE event_type = 'VIEW'
GROUP BY product_id
ORDER BY view_count DESC;

-- Q2. Most active customers
-- "Which customers generate the most activity on the website?"
SELECT customer_id, COUNT(*) AS activity_count
FROM website_activity
WHERE customer_id != 'GUEST'
GROUP BY customer_id
ORDER BY activity_count DESC
LIMIT 10;

-- Q3. Events by type
-- "What is the distribution of event types?"
SELECT event_type, COUNT(*) AS count
FROM website_activity
GROUP BY event_type
ORDER BY count DESC;

-- Q4. Activity by device
-- "Which devices do customers use most?"
SELECT device, COUNT(*) AS count
FROM website_activity
GROUP BY device
ORDER BY count DESC;

-- Q5. Activity by location
-- "Where are our customers located?"
SELECT location, COUNT(*) AS count
FROM website_activity
GROUP BY location
ORDER BY count DESC;

-- Q6. Conversion funnel: products with most purchases
-- (non-web analytics derived from the same table)
SELECT product_id, COUNT(*) AS purchase_events
FROM website_activity
WHERE event_type = 'PURCHASE'
GROUP BY product_id
ORDER BY purchase_events DESC;
