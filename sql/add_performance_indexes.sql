DROP INDEX IF EXISTS idx_fact_date;
DROP INDEX IF EXISTS idx_fact_product;
DROP INDEX IF EXISTS idx_fact_customer;
DROP INDEX IF EXISTS idx_fact_store;
DROP INDEX IF EXISTS idx_fact_order;

-- Foreign-key columns of the fact table (the ones used in JOIN/WHERE)
CREATE INDEX idx_fact_date     ON FactSales(date_id);
CREATE INDEX idx_fact_product  ON FactSales(product_id);
CREATE INDEX idx_fact_customer ON FactSales(customer_id);
CREATE INDEX idx_fact_store    ON FactSales(store_id);
CREATE INDEX idx_fact_order    ON FactSales(order_id);

-- Composite index used by "sales by month" (Q2) groups.
DROP INDEX IF EXISTS idx_fact_date_customer;
CREATE INDEX idx_fact_date_customer ON FactSales(date_id, customer_id);

-- ============================================================================
-- Inspect the indexes on our schema
-- ============================================================================
SELECT tablename,
       indexname,
       indexdef
FROM pg_indexes
WHERE schemaname = 'public'
ORDER BY tablename, indexname;

-- ============================================================================
-- Evidence that the index is used (PostgreSQL query planner)
-- ============================================================================
EXPLAIN ANALYZE
SELECT SUM(fs.sales_amount)
FROM FactSales fs
WHERE fs.date_id BETWEEN 20250401 AND 20250501;

EXPLAIN ANALYZE
SELECT p.category, SUM(fs.sales_amount)
FROM FactSales fs
JOIN DimProduct p ON fs.product_id = p.product_id
GROUP BY p.category;
