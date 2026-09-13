#!/usr/bin/env bash
# ============================================================================
# entrypoint.sh
# Starts HDFS (namenode + datanode) in single-node mode, then drops into
# an interactive bash shell so the user can run HDFS/MapReduce/Hive/Pig
# commands from the assignment.
#
# First time only: formats the namenode directory.
# Subsequent starts: skips formatting to preserve existing data.
# ============================================================================
set -euo pipefail

HADOOP_HOME=${HADOOP_HOME:-/opt/hadoop}
HIVE_HOME=${HIVE_HOME:-/opt/hive}
PIG_HOME=${PIG_HOME:-/opt/pig}
DATA_DIR=/workspace/data
RAW_DIR=/user/hadoop/raw
OUTPUT_DIR=/user/hadoop/output

export JAVA_HOME=${JAVA_HOME:-/usr/lib/jvm/java-11-openjdk-amd64}
export PATH=${HADOOP_HOME}/bin:${HADOOP_HOME}/sbin:${HIVE_HOME}/bin:${PIG_HOME}/bin:$PATH

# Format namenode only on first boot (no /tmp/hadoop-data exists)
if [ ! -d /tmp/hadoop-root/dfs/name ]; then
    echo ">>> Formatting HDFS namenode (first boot)..."
    hdfs namenode -format -nonInteractive -force
fi

echo ">>> Starting HDFS (namenode + datanode)..."
start-dfs.sh

echo ">>> Waiting for HDFS to become ready..."
until hdfs dfs -ls / > /dev/null 2>&1; do sleep 2; done
echo "    HDFS is ready."

# Create directories for the raw data and output
hdfs dfs -mkdir -p ${RAW_DIR} 2>/dev/null || true
hdfs dfs -mkdir -p ${OUTPUT_DIR} 2>/dev/null || true

# Upload the website activity CSV if present in the mounted data directory
CSV_FILE=${DATA_DIR}/website_activity.csv
if [ -f "${CSV_FILE}" ]; then
    # Remove existing copy if present (idempotent)
    hdfs dfs -rm -f ${RAW_DIR}/website_activity.csv 2>/dev/null || true
    hdfs dfs -put "${CSV_FILE}" ${RAW_DIR}/website_activity.csv
    echo ">>> Uploaded ${CSV_FILE} -> HDFS ${RAW_DIR}/website_activity.csv"
fi

# Show HDFS status
echo ""
echo ">>> HDFS directory listing:"
hdfs dfs -ls -R ${RAW_DIR}  2>/dev/null || true
echo ""
echo ">>> HDFS status:"
hdfs dfsadmin -report 2>/dev/null | head -12

echo ""
echo "================================================================"
echo "  Ready.  You are inside the Hadoop single-node container."
echo "  All commands (hdfs, hadoop, hive, pig, mapper.py, reducer.py)"
echo "  are available at the shell prompt."
echo ""
echo "  Quick test:"
echo "    hdfs dfs -ls ${RAW_DIR}"
echo "    hadoop jar ${HADOOP_HOME}/share/hadoop/tools/lib/hadoop-streaming-*.jar \\"
echo "        -mapper  /workspace/hadoop/mapreduce/mapper.py \\"
echo "        -reducer /workspace/hadoop/mapreduce/reducer.py \\"
echo "        -input   ${RAW_DIR}/website_activity.csv \\"
echo "        -output  ${OUTPUT_DIR}/product_views"
echo "================================================================"
echo ""

# Drop into interactive shell
exec /bin/bash