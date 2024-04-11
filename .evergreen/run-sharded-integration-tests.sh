#!/bin/bash

set -o xtrace   # Write all commands first to stderr
set -o errexit  # Exit the script with error if any of the commands fail

# Supported/used environment variables:
# MONGODB_URI     Set the suggested connection MONGODB_URI (including credentials and topology info)
# SCALA_VERSION   The Scala version to compile with
# SPARK_VERSION   The spark version to test against
MONGODB_URI=${MONGODB_URI:-}
SCALA_VERSION=${SCALA_VERSION:-2.12}
SPARK_VERSION=${SPARK_VERSION:-3.1.2}

export JAVA_HOME="/opt/java/jdk11"

############################################
#            Main Program                  #
############################################


echo "Running tests connecting to $MONGODB_URI on JDK${JAVA_VERSION}"

./gradlew -version
./gradlew -Dorg.mongodb.test.uri=${MONGODB_URI} --stacktrace --info integrationTest --tests "com.mongodb.spark.sql.connector.read.partitioner.ShardedPartitionerTest" -DscalaVersion=$SCALA_VERSION -DsparkVersion=$SPARK_VERSION
