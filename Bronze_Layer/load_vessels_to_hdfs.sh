#!/bin/bash

set -e

source "$(dirname "$0")/config.sh"

REMOTE_DIR="data/vessels"
LOCAL_DIR="/tmp/vessels"
HDFS_BASE="${HDFS_BASE_PATH}/vessels"

mkdir -p "${LOCAL_DIR}"

echo "[$(date)] Starting Vessels ingestion..."
echo "[$(date)] Remote directory: ${REMOTE_DIR}"

# Download all CSV files from SFTP
sshpass -p "${SFTP_PASSWORD}" sftp \
    -o StrictHostKeyChecking=no \
    -P "${SFTP_PORT}" \
    "${SFTP_USER}@${SFTP_HOST}" <<EOF
mget ${REMOTE_DIR}/*.csv ${LOCAL_DIR}/
EOF

# Check if files were downloaded
FILES=$(find "${LOCAL_DIR}" -maxdepth 1 -type f -name "vessels_*.csv")

if [ -z "${FILES}" ]; then
    echo "[$(date)] No vessels CSV files found."
    exit 0
fi

# Process every downloaded file
for LOCAL_PATH in ${FILES}; do

    FILENAME=$(basename "${LOCAL_PATH}")

    # Extract date from filename
    FILE_DATE=$(echo "${FILENAME}" | sed -n 's/vessels_\([0-9]\{4\}-[0-9]\{2\}-[0-9]\{2\}\)\.csv/\1/p')

    if [ -z "${FILE_DATE}" ]; then
        echo "[$(date)] Skipping invalid filename: ${FILENAME}"
        continue
    fi

    YEAR="${FILE_DATE:0:4}"
    MONTH="${FILE_DATE:5:2}"

    HDFS_PATH="${HDFS_BASE}/year=${YEAR}/month=${MONTH}"

    echo "----------------------------------------"
    echo "[$(date)] Processing: ${FILENAME}"
    echo "[$(date)] HDFS path: ${HDFS_PATH}"

    # Create HDFS partition
    hdfs dfs -mkdir -p "${HDFS_PATH}"

    # Upload to HDFS
    hdfs dfs -put -f "${LOCAL_PATH}" "${HDFS_PATH}/"

    echo "[$(date)] Loaded successfully: ${HDFS_PATH}/${FILENAME}"

    # Remove temporary file
    rm -f "${LOCAL_PATH}"

done

echo "----------------------------------------"
echo "[$(date)] Vessels ingestion completed successfully."