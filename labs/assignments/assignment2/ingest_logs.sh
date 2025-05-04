#!/bin/bash

# Check if date parameter is provided
if [ -z "$1" ]; then
  echo "Usage: $0 YYYY-MM-DD"
  exit 1
fi

# Parse the date parameter
DATE=$1
YEAR=$(echo $DATE | cut -d'-' -f1)
MONTH=$(echo $DATE | cut -d'-' -f2)
DAY=$(echo $DATE | cut -d'-' -f3)

# Define HDFS directories
LOG_DIR="/raw/logs/$YEAR/$MONTH/$DAY"
META_DIR="/raw/metadata/$YEAR/$MONTH/$DAY"

# Create directories in HDFS
hdfs dfs -mkdir -p $LOG_DIR
# Expected output: Directory /raw/logs/<year>/<month>/<day> created successfully
hdfs dfs -mkdir -p $META_DIR
# Expected output: Directory /raw/metadata/<year>/<month>/<day> created successfully

# Copy log file for the given date into HDFS
hdfs dfs -put ./raw_data/$DATE.csv $LOG_DIR/
# Expected output: File $DATE.csv uploaded to /raw/logs/<year>/<month>/<day>

# Copy content metadata file into HDFS
hdfs dfs -put ./raw_data/content_metadata.csv $META_DIR/
# Expected output: File content_metadata.csv uploaded to /raw/metadata/<year>/<month>/<day>

echo "Data ingested for $DATE into $LOG_DIR and $META_DIR"
# Expected output: Data ingested for <date> into /raw/logs/<year>/<month>/<day> and /raw/metadata/<year>/<month>/<day>