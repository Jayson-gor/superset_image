#!/bin/bash

# Function to wait for a service to be available
function wait_for_it() {
    local serviceport=$1
    local service=${serviceport%%:*}
    local port=${serviceport#*:}
    local retry_seconds=5
    local max_try=100
    let i=1

    nc -z $service $port
    result=$?

    until [ $result -eq 0 ]; do
        echo "[$i/$max_try] Waiting for ${service}:${port}..."
        if (( $i == $max_try )); then
            echo "[$i/$max_try] ${service}:${port} still not available; giving up after ${max_try} tries."
            exit 1
        fi
        echo "[$i/$max_try] Retrying in ${retry_seconds}s..."
        let "i++"
        sleep $retry_seconds
        nc -z $service $port
        result=$?
    done
    echo "[$i/$max_try] ${service}:${port} is available."
}

# Start SSH service
sudo service ssh start

# Configure SSH for passwordless access
ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
chmod 0600 ~/.ssh/authorized_keys
echo "StrictHostKeyChecking no" >> ~/.ssh/config

# Ensure HDFS directories are writable
sudo mkdir -p /opt/hadoop/dfs/name /opt/hadoop/dfs/data
sudo chown -R jupyter:jupyter /opt/hadoop/dfs

# Set JAVA_HOME globally and ensure SSH inherits it
echo "export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64" | sudo tee -a /etc/environment
echo "export PATH=$JAVA_HOME/bin:$PATH" | sudo tee -a /etc/environment
source /etc/environment

# Format HDFS if not already formatted
if [ ! -d "/opt/hadoop/dfs/name/current" ]; then
    $HADOOP_HOME/bin/hdfs namenode -format
fi

# Start HDFS
$HADOOP_HOME/sbin/start-dfs.sh

# Wait for HDFS NameNode to be ready
wait_for_it localhost:9000

# Start YARN
$HADOOP_HOME/sbin/start-yarn.sh

# Wait for YARN ResourceManager to be ready
wait_for_it localhost:8088

# Upload Spark jars to HDFS
$HADOOP_HOME/bin/hdfs dfs -mkdir -p /spark/jars
$HADOOP_HOME/bin/hdfs dfs -put $SPARK_HOME/jars/* /spark/jars/

# Upload dataset to HDFS
if [ -d "/app/data" ]; then
    $HADOOP_HOME/bin/hdfs dfs -mkdir -p /input
    $HADOOP_HOME/bin/hdfs dfs -put /app/data/* /input/
fi

# Start JupyterLab
jupyter lab --ip=0.0.0.0 --port=8888 --no-browser --allow-root