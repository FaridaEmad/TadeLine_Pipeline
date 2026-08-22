#!/bin/bash

set -e

echo "======================================"
echo "Starting ITVDELAB"
echo "======================================"

# Start the original ITVDELAB startup script.
# DO NOT modify /deploy.sh.
# It starts Hadoop, Spark and Jupyter.
echo "Starting /deploy.sh..."

 /deploy.sh &
DEPLOY_PID=$!

echo "deploy.sh PID: ${DEPLOY_PID}"

echo "======================================"
echo "Waiting for Hadoop"
echo "======================================"

# Wait for NameNode
until jps | grep -q "NameNode"; do
    echo "Waiting for NameNode..."
    sleep 5
done

echo "NameNode is running."

# Wait for ResourceManager
until jps | grep -q "ResourceManager"; do
    echo "Waiting for ResourceManager..."
    sleep 5
done

echo "ResourceManager is running."

# Wait for DataNode
until jps | grep -q "DataNode"; do
    echo "Waiting for DataNode..."
    sleep 5
done

echo "DataNode is running."

echo "======================================"
echo "Hadoop is ready"
echo "======================================"

# Give HDFS a little time to become fully responsive.
sleep 10

echo "======================================"
echo "Applying custom Hive configuration"
echo "======================================"

# /deploy.sh copies the base Hive configuration during initialization.
# Apply our configuration after that initialization.
cp /opt/custom-hive-site.xml /opt/hive/conf/hive-site.xml

echo "Hive configuration installed."

echo "======================================"
echo "Starting Hive Metastore"
echo "======================================"

hive --service metastore \
    > /logs/hive-metastore.log 2>&1 &

METASTORE_PID=$!

echo "Hive Metastore PID: ${METASTORE_PID}"

# Give Metastore time to bind to port 9083.
sleep 10

echo "======================================"
echo "Starting HiveServer2"
echo "======================================"

hive --service hiveserver2 \
    > /logs/hiveserver2.log 2>&1 &

HIVESERVER2_PID=$!

echo "HiveServer2 PID: ${HIVESERVER2_PID}"

echo "======================================"
echo "ITVDELAB startup completed"
echo "======================================"

echo "Hadoop:"
echo "  NameNode        : 9870"
echo "  ResourceManager : 8088"

echo "Hive:"
echo "  Metastore       : 9083"
echo "  HiveServer2     : 10000"

echo "Logs:"
echo "  /logs/hive-metastore.log"
echo "  /logs/hiveserver2.log"

echo "======================================"

# Keep the wrapper process alive.
# /deploy.sh itself keeps Jupyter alive.
wait