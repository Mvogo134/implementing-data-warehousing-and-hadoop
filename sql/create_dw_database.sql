
-- Re-create cleanly (idempotent for re-runs)
\connect postgres
DROP DATABASE IF EXISTS ecommerce_dw;
CREATE DATABASE ecommerce_dw;

-- Connect to the new warehouse
\connect ecommerce_dw

-- Sanity check
SELECT current_database(), current_user;
