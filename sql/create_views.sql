CREATE OR REPLACE VIEW vw_MonthlySales AS
SELECT d.year,
       d.month,
       d.month_name,
       SUM(fs.sales_amount)                 AS sales_usd,
       COUNT(DISTINCT fs.order_id)          AS orders,
       COUNT(*)                             AS order_lines,
       ROUND(SUM(fs.sales_amount) / NULLIF(COUNT(DISTINCT fs.order_id), 0), 2)
                                           AS avg_order_value_usd
FROM FactSales fs
JOIN DimDate d ON fs.date_id = d.date_id
GROUP BY d.year, d.month, d.month_name;

-- ---------------------------------------------------------------------------
-- View 2: vw_ProductPerformance
-- Sales, units, orders and discounts per product.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW vw_ProductPerformance AS
SELECT p.product_id,
       p.product_name,
       p.category,
       p.subcategory,
       p.brand,
       SUM(fs.sales_amount)                  AS revenue_usd,
       SUM(fs.quantity)                      AS units_sold,
       COUNT(DISTINCT fs.order_id)           AS orders,
       ROUND(AVG(fs.unit_price), 2)          AS avg_unit_price_usd,
       ROUND(AVG(fs.discount_rate) * 100, 1) AS avg_discount_pct
FROM FactSales fs
JOIN DimProduct p ON fs.product_id = p.product_id
GROUP BY p.product_id, p.product_name, p.category, p.subcategory, p.brand;

-- ---------------------------------------------------------------------------
-- View 3: vw_CustomerSalesSummary
-- Lifetime spend and engagement per customer.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW vw_CustomerSalesSummary AS
SELECT c.customer_id,
       c.full_name,
       c.city,
       c.country,
       c.region,
       COUNT(DISTINCT fs.order_id)          AS orders,
       SUM(fs.quantity)                     AS units_purchased,
       SUM(fs.sales_amount)                 AS lifetime_value_usd,
       MIN(d.full_date)                     AS first_order_date,
       MAX(d.full_date)                     AS last_order_date
FROM FactSales fs
JOIN DimCustomer c ON fs.customer_id = c.customer_id
JOIN DimDate d     ON fs.date_id    = d.date_id
GROUP BY c.customer_id, c.full_name, c.city, c.country, c.region;

-- ---------------------------------------------------------------------------
-- Bonus: vw_StoreChannelPerformance
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW vw_StoreChannelPerformance AS
SELECT s.channel,
       s.store_id,
       s.store_name,
       COUNT(DISTINCT fs.order_id)          AS orders,
       SUM(fs.sales_amount)                 AS sales_usd
FROM FactSales fs
JOIN DimStore s ON fs.store_id = s.store_id
GROUP BY s.channel, s.store_id, s.store_name;

-- ============================================================================
-- Demonstrate the views (also see 08_results_queries.sql)
-- ============================================================================

-- 1. Monthly sales (top of view)
SELECT * FROM vw_MonthlySales ORDER BY year, month;

-- 2. Best performing products through the view
SELECT product_id, product_name, revenue_usd, units_sold
FROM vw_ProductPerformance
ORDER BY revenue_usd DESC
LIMIT 5;

-- 3. Highest-value customer through the view
SELECT customer_id, full_name, region, lifetime_value_usd, orders
FROM vw_CustomerSalesSummary
ORDER BY lifetime_value_usd DESC
LIMIT 5;

-- 4. Channel performance through the view
SELECT channel, COUNT(*) AS stores, SUM(sales_usd) AS total_sales_usd
FROM vw_StoreChannelPerformance
GROUP BY channel
ORDER BY total_sales_usd DESC;
