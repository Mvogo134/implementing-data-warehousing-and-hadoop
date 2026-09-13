#!/usr/bin/env bash

set -euo pipefail

# ---------------------------------------------------------------------------
# Configuration  (override via environment or command-line args)
# ---------------------------------------------------------------------------
PG_USER="${1:-${PG_USER:-user}}"
PG_HOST="${2:-${PG_HOST:-localhost}}"
PG_PORT="${3:-${PG_PORT:-5432}}"
export PGPASSWORD="${PG_PASSWORD:-password}"

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PSQL="psql -h ${PG_HOST} -p ${PG_PORT} -U ${PG_USER} -X -A"
GREEN='\033[0;32m'
NC='\033[0m'

header() { echo -e "\n${GREEN}==========  $*  ==========${NC}"; }

# ---------------------------------------------------------------------------
header "STEP 1 — Generate sample data"
# ---------------------------------------------------------------------------
cd "${PROJECT_ROOT}"
python3 scripts/generate_ecommerce_data.py
echo "  Generated sql/seed_warehouse_data.sql and hadoop/data/website_activity.csv"

# ---------------------------------------------------------------------------
header "STEP 2 — Create the data warehouse in PostgreSQL"
# ---------------------------------------------------------------------------
${PSQL} -d postgres -f sql/create_dw_database.sql >/dev/null 2>&1 && echo "  Database ecommerce_dw created."
${PSQL} -d ecommerce_dw -f sql/create_star_schema.sql >/dev/null 2>&1 && echo "  Star schema created."

# ---------------------------------------------------------------------------
header "STEP 3 — Load sample data"
# ---------------------------------------------------------------------------
${PSQL} -d ecommerce_dw -f sql/seed_warehouse_data.sql >/dev/null 2>&1 && echo "  Dimensions + FactSales loaded (165 fact rows)."

# Verify
${PSQL} -d ecommerce_dw -c "
SELECT 'DimDate'     AS tbl, COUNT(*) FROM DimDate     UNION ALL
SELECT 'DimCustomer', COUNT(*) FROM DimCustomer         UNION ALL
SELECT 'DimProduct',  COUNT(*) FROM DimProduct          UNION ALL
SELECT 'DimStore',    COUNT(*) FROM DimStore            UNION ALL
SELECT 'FactSales',   COUNT(*) FROM FactSales           ORDER BY 1;"

# ---------------------------------------------------------------------------
header "STEP 4 — SQL analytical queries"
# ---------------------------------------------------------------------------
echo "  Running 12 analytical queries..."
${PSQL} -d ecommerce_dw -f sql/business_analytics.sql | tail -30

# ---------------------------------------------------------------------------
header "STEP 5 — SQL views"
# ---------------------------------------------------------------------------
${PSQL} -d ecommerce_dw -f sql/create_views.sql >/dev/null 2>&1 && echo "  4 views created."

# ---------------------------------------------------------------------------
header "STEP 6 — SQL indexes"
# ---------------------------------------------------------------------------
${PSQL} -d ecommerce_dw -f sql/add_performance_indexes.sql >/dev/null 2>&1 && echo "  6 indexes created."
${PSQL} -d ecommerce_dw -c "SELECT indexname FROM pg_indexes WHERE tablename='factsales';"

# ---------------------------------------------------------------------------
header "STEP 7 — Export DW product sales for hybrid analysis"
# ---------------------------------------------------------------------------
mkdir -p "${PROJECT_ROOT}/results"
${PSQL} -d ecommerce_dw <<'SQL'
\copy (SELECT p.product_id, p.product_name, p.category,
              COUNT(*) AS purchases, SUM(fs.quantity) AS units_sold,
              SUM(fs.sales_amount) AS revenue_usd
       FROM FactSales fs
       JOIN DimProduct p ON fs.product_id = p.product_id
       GROUP BY p.product_id, p.product_name, p.category
       ORDER BY p.product_id)
TO 'results/warehouse_product_summary.csv' WITH CSV HEADER
SQL
echo "  warehouse_product_summary.csv exported."

# ---------------------------------------------------------------------------
header "STEP 8 — Local MapReduce (Hadoop Streaming simulation)"
# ---------------------------------------------------------------------------
echo "  Running mapper | sort | reducer pipeline locally..."
cat hadoop/data/website_activity.csv |
    python3 hadoop/mapreduce/mapper.py |
    sort |
    python3 hadoop/mapreduce/reducer.py |
    sort -k2 -rn \
        >results/mapreduce_product_views.txt
echo "  Top 5 products by views:"
head -5 results/mapreduce_product_views.txt

# ---------------------------------------------------------------------------
header "STEP 9 — Hybrid analysis (data warehouse + Hadoop)"
# ---------------------------------------------------------------------------
python3 scripts/hybrid_analysis.py

# ---------------------------------------------------------------------------
header "STEP 10 (Optional) — Hadoop Docker environment"
# ---------------------------------------------------------------------------
echo "  To build and run the Hadoop Docker container:"
echo "    cd docker"
echo "    docker compose up -d --build"
echo "    docker compose exec hadoop bash"
echo "  Then inside the container run:"
echo "    hadoop jar /opt/hadoop/share/hadoop/tools/lib/hadoop-streaming-*.jar \\"
echo "        -mapper  /workspace/hadoop/mapreduce/mapper.py \\"
echo "        -reducer /workspace/hadoop/mapreduce/reducer.py \\"
echo "        -input   /user/hadoop/raw/website_activity.csv \\"
echo "        -output  /user/hadoop/output/product_views"
echo "    hive -f /workspace/hadoop/hive/web_activity_analytics.sql"
echo "    pig -x local /workspace/hadoop/pig/transform_activity.pig"
echo "    docker compose down   # stop when done"

header "ALL DONE"
echo "  Results are in:  results/"
echo "  SQL files are in: sql/"
echo "  Hadoop files are in: hadoop/"
echo "  Docker specs are in: docker/"

