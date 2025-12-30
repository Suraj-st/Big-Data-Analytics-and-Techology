#!/bin/bash

# --------------------------
# Config
# --------------------------
INPUT_PATH=$1          # HDFS input file, e.g., /input/email-EuAll.txt
OUTPUT_PATH=$2         # HDFS output dir, e.g., /output/indegree_output
MAPPER=$3              # Local mapper script, e.g., /mapper_in_degree.py
REDUCER=$4             # Local reducer script, e.g., /reducer_in_degree.py
LOG_DIR=./logs         # Directory to store logs
mkdir -p $LOG_DIR

# --------------------------
# Start system metrics logging
# --------------------------
echo "Starting system monitoring..."
# Memory & CPU usage
top -b -d 1 > $LOG_DIR/top.log &
TOP_PID=$!
# Disk I/O
iostat -x 1 > $LOG_DIR/iostat.log &
IOSTAT_PID=$!
# Network usage
ifstat -t 1 > $LOG_DIR/network.log &
IFSTAT_PID=$!

# --------------------------
# Run Hadoop job with timing
# --------------------------
echo "Running Hadoop job..."
START_TIME=$(date +%s)

hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
  -input $INPUT_PATH \
  -output $OUTPUT_PATH \
  -mapper "python3 $MAPPER" \
  -reducer "python3 $REDUCER"

END_TIME=$(date +%s)
ELAPSED=$((END_TIME-START_TIME))
echo "Execution Time: $ELAPSED seconds" | tee $LOG_DIR/execution_time.log

# --------------------------
# Stop monitoring
# --------------------------
kill $TOP_PID $IOSTAT_PID $IFSTAT_PID

echo "Job finished. Logs saved in $LOG_DIR"
