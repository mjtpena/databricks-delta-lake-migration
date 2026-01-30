#!/bin/bash

# Run Delta Lake performance benchmarks
# This script executes benchmarks on Databricks

set -e

echo "========================================="
echo "Delta Lake Performance Benchmark Runner"
echo "========================================="

# Check environment variables
if [ -z "$DATABRICKS_HOST" ]; then
    echo "Error: DATABRICKS_HOST environment variable not set"
    exit 1
fi

if [ -z "$DATABRICKS_TOKEN" ]; then
    echo "Error: DATABRICKS_TOKEN environment variable not set"
    exit 1
fi

# Configuration
CLUSTER_ID=${CLUSTER_ID:-""}
NUM_ROWS=${NUM_ROWS:-10000000}
BENCHMARK_SCRIPT="delta_performance_benchmark.py"

echo "Configuration:"
echo "  Databricks Host: $DATABRICKS_HOST"
echo "  Cluster ID: $CLUSTER_ID"
echo "  Number of Rows: $NUM_ROWS"
echo ""

# Upload benchmark script to DBFS
echo "Uploading benchmark script to DBFS..."
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
DBFS_PATH="/tmp/benchmarks/delta_benchmark_${TIMESTAMP}.py"

# Create DBFS directory
curl -X POST \
    "${DATABRICKS_HOST}/api/2.0/dbfs/mkdirs" \
    -H "Authorization: Bearer ${DATABRICKS_TOKEN}" \
    -H "Content-Type: application/json" \
    -d "{\"path\": \"/tmp/benchmarks\"}"

# Upload script (using base64 encoding)
ENCODED_SCRIPT=$(base64 < "$BENCHMARK_SCRIPT")

curl -X POST \
    "${DATABRICKS_HOST}/api/2.0/dbfs/put" \
    -H "Authorization: Bearer ${DATABRICKS_TOKEN}" \
    -H "Content-Type: application/json" \
    -d "{
        \"path\": \"${DBFS_PATH}\",
        \"contents\": \"${ENCODED_SCRIPT}\",
        \"overwrite\": true
    }"

echo "Script uploaded to: ${DBFS_PATH}"

# Run benchmark
echo ""
echo "Running benchmark..."

JOB_CONFIG=$(cat <<EOF
{
    "name": "Delta Performance Benchmark ${TIMESTAMP}",
    "tasks": [
        {
            "task_key": "benchmark_task",
            "spark_python_task": {
                "python_file": "dbfs:${DBFS_PATH}"
            },
            "existing_cluster_id": "${CLUSTER_ID}",
            "timeout_seconds": 7200
        }
    ]
}
EOF
)

# Create and run job
JOB_RESPONSE=$(curl -X POST \
    "${DATABRICKS_HOST}/api/2.0/jobs/create" \
    -H "Authorization: Bearer ${DATABRICKS_TOKEN}" \
    -H "Content-Type: application/json" \
    -d "$JOB_CONFIG")

JOB_ID=$(echo "$JOB_RESPONSE" | jq -r '.job_id')
echo "Created job: ${JOB_ID}"

# Run job
RUN_RESPONSE=$(curl -X POST \
    "${DATABRICKS_HOST}/api/2.0/jobs/run-now" \
    -H "Authorization: Bearer ${DATABRICKS_TOKEN}" \
    -H "Content-Type: application/json" \
    -d "{\"job_id\": ${JOB_ID}}")

RUN_ID=$(echo "$RUN_RESPONSE" | jq -r '.run_id')
echo "Started run: ${RUN_ID}"

# Monitor run status
echo ""
echo "Monitoring benchmark execution..."

while true; do
    RUN_STATUS=$(curl -s -X GET \
        "${DATABRICKS_HOST}/api/2.0/jobs/runs/get?run_id=${RUN_ID}" \
        -H "Authorization: Bearer ${DATABRICKS_TOKEN}")

    STATE=$(echo "$RUN_STATUS" | jq -r '.state.life_cycle_state')
    RESULT=$(echo "$RUN_STATUS" | jq -r '.state.result_state // "RUNNING"')

    echo "  Status: ${STATE} - ${RESULT}"

    if [ "$STATE" = "TERMINATED" ]; then
        if [ "$RESULT" = "SUCCESS" ]; then
            echo ""
            echo "✓ Benchmark completed successfully!"
        else
            echo ""
            echo "✗ Benchmark failed: ${RESULT}"
            exit 1
        fi
        break
    fi

    sleep 10
done

# Cleanup
echo ""
echo "Cleaning up..."
curl -X POST \
    "${DATABRICKS_HOST}/api/2.0/jobs/delete" \
    -H "Authorization: Bearer ${DATABRICKS_TOKEN}" \
    -H "Content-Type: application/json" \
    -d "{\"job_id\": ${JOB_ID}}"

echo "Cleanup complete"
echo ""
echo "========================================="
echo "Benchmark execution finished"
echo "========================================="
