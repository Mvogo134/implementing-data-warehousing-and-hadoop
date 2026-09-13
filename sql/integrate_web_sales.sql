

SELECT p.product_id,
       p.product_name,
       p.category,
       COUNT(*)                          AS purchases,
       SUM(fs.quantity)                  AS units_sold,
       SUM(fs.sales_amount)              AS revenue_usd
FROM FactSales fs
JOIN DimProduct p ON fs.product_id = p.product_id
GROUP BY p.product_id, p.product_name, p.category
ORDER BY p.product_id;

-- ============================================================================
-- Standalone SPOOL-friendly version printed once for the README results file.
-- (psql users: \copy ... TO 'results/warehouse_product_summary.csv' CSV HEADER)
-- ============================================================================
\echo '--- DW product purchase totals (used by hybrid_analysis.py) ---'
SELECT p.product_id, p.product_name,
       COUNT(*) AS purchases, SUM(fs.sales_amount) AS revenue_usd
FROM FactSales fs
JOIN DimProduct p ON fs.product_id = p.product_id
GROUP BY p.product_id, p.product_name
ORDER BY p.product_id;
