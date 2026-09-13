
SELECT COUNT(*)                              AS total_orders_lines,
       SUM(sales_amount)                     AS total_sales_usd,
       ROUND(AVG(sales_amount), 2)           AS avg_sale_usd,
       MIN(sales_amount)                     AS smallest_sale_usd,
       MAX(sales_amount)                     AS largest_sale_usd,
       SUM(quantity)                         AS total_units_sold
FROM FactSales;

-- ---------------------------------------------------------------------------
-- Q2. Monthly sales
-- Business question: What is our month-by-month revenue trend?
-- ---------------------------------------------------------------------------
SELECT d.year,
       d.month,
       d.month_name,
       COUNT(*)                             AS order_lines,
       SUM(fs.sales_amount)                 AS monthly_sales_usd
FROM FactSales fs
JOIN DimDate d ON fs.date_id = d.date_id
GROUP BY d.year, d.month, d.month_name
ORDER BY d.year, d.month;

-- ---------------------------------------------------------------------------
-- Q3. Yearly sales (with year-over-year view)
-- Business question: How does annual performance compare?
-- ---------------------------------------------------------------------------
SELECT d.year,
       SUM(fs.sales_amount)  AS yearly_sales_usd,
       COUNT(DISTINCT fs.order_id) AS orders
FROM FactSales fs
JOIN DimDate d ON fs.date_id = d.date_id
GROUP BY d.year
ORDER BY d.year;

-- ---------------------------------------------------------------------------
-- Q4. Top-selling products
-- Business question: Which products generate the most revenue?
-- ---------------------------------------------------------------------------
SELECT p.product_id,
       p.product_name,
       p.category,
       SUM(fs.sales_amount)          AS revenue_usd,
       SUM(fs.quantity)              AS units_sold,
       COUNT(DISTINCT fs.order_id)   AS orders
FROM FactSales fs
JOIN DimProduct p ON fs.product_id = p.product_id
GROUP BY p.product_id, p.product_name, p.category
ORDER BY revenue_usd DESC
LIMIT 10;

-- ---------------------------------------------------------------------------
-- Q5. Top product categories
-- Business question: Which product categories contribute most to revenue?
-- ---------------------------------------------------------------------------
SELECT p.category,
       SUM(fs.sales_amount)  AS category_revenue_usd,
       SUM(fs.quantity)      AS units_sold,
       COUNT(*)              AS line_items
FROM FactSales fs
JOIN DimProduct p ON fs.product_id = p.product_id
GROUP BY p.category
ORDER BY category_revenue_usd DESC;

-- ---------------------------------------------------------------------------
-- Q6. Highest-value customers
-- Business question: Which customers contribute the most revenue?
-- ---------------------------------------------------------------------------
SELECT c.customer_id,
       c.full_name,
       c.city,
       c.country,
       c.region,
       SUM(fs.sales_amount)   AS customer_lifetime_value_usd,
       COUNT(DISTINCT fs.order_id) AS orders_placed
FROM FactSales fs
JOIN DimCustomer c ON fs.customer_id = c.customer_id
GROUP BY c.customer_id, c.full_name, c.city, c.country, c.region
ORDER BY customer_lifetime_value_usd DESC
LIMIT 10;

-- ---------------------------------------------------------------------------
-- Q7. Sales by geographic region
-- Business question: Which regions generate the most revenue?
-- ---------------------------------------------------------------------------
SELECT c.region,
       c.country,
       SUM(fs.sales_amount)  AS sales_usd,
       COUNT(*)              AS line_items
FROM FactSales fs
JOIN DimCustomer c ON fs.customer_id = c.customer_id
GROUP BY c.region, c.country
ORDER BY sales_usd DESC;

-- ---------------------------------------------------------------------------
-- Q8. Sales by store / sales channel
-- Business question: Which channels and stores perform best?
-- ---------------------------------------------------------------------------
SELECT s.store_id,
       s.store_name,
       s.channel,
       s.city,
       s.country,
       SUM(fs.sales_amount)   AS sales_usd,
       COUNT(DISTINCT fs.order_id) AS orders
FROM FactSales fs
JOIN DimStore s ON fs.store_id = s.store_id
GROUP BY s.store_id, s.store_name, s.channel, s.city, s.country
ORDER BY sales_usd DESC;

-- ---------------------------------------------------------------------------
-- Q9. Orders per customer with average order value
-- Business question: How engaged are our customers on average?
-- Demonstrates AVG() inside a subquery and aggregate-on-aggregate.
-- ---------------------------------------------------------------------------
SELECT c.customer_id,
       c.full_name,
       COUNT(DISTINCT fs.order_id)                          AS orders,
       SUM(fs.sales_amount)                                 AS total_spend,
       ROUND(SUM(fs.sales_amount) / COUNT(DISTINCT fs.order_id), 2)
                                                           AS avg_order_value_usd
FROM FactSales fs
JOIN DimCustomer c ON fs.customer_id = c.customer_id
GROUP BY c.customer_id, c.full_name
HAVING COUNT(DISTINCT fs.order_id) >= 2
ORDER BY avg_order_value_usd DESC;

-- ---------------------------------------------------------------------------
-- Q10. Weekend vs weekday performance
-- Business question: Do customers buy more at weekends?
-- Demonstrates a conditional expression inside SUM().
-- ---------------------------------------------------------------------------
SELECT d.is_weekend,
       COUNT(*)                              AS line_items,
       SUM(fs.sales_amount)                  AS sales_usd,
       ROUND(AVG(fs.sales_amount), 2)        AS avg_sale_usd
FROM FactSales fs
JOIN DimDate d ON fs.date_id = d.date_id
GROUP BY d.is_weekend
ORDER BY d.is_weekend;

-- ---------------------------------------------------------------------------
-- Q11. Monthly sales that exceed 10 order lines
-- Business question: Which months were active enough to matter?
-- Demonstrates HAVING with COUNT().
-- ---------------------------------------------------------------------------
SELECT d.year, d.month, d.month_name,
       COUNT(*)          AS order_lines,
       SUM(fs.sales_amount) AS sales_usd
FROM FactSales fs
JOIN DimDate d ON fs.date_id = d.date_id
GROUP BY d.year, d.month, d.month_name
HAVING COUNT(*) > 10
ORDER BY d.year, d.month;

-- ---------------------------------------------------------------------------
-- Q12. Discount analysis per category
-- Business question: How much discounting happens per category?
-- ---------------------------------------------------------------------------
SELECT p.category,
       COUNT(*)                                   AS lines_with_discount,
       ROUND(AVG(fs.discount_rate) * 100, 1)      AS avg_discount_pct,
       SUM(fs.discount_rate * fs.unit_price * fs.quantity) AS discount_value_usd
FROM FactSales fs
JOIN DimProduct p ON fs.product_id = p.product_id
WHERE fs.discount_rate > 0
GROUP BY p.category
ORDER BY discount_value_usd DESC;
