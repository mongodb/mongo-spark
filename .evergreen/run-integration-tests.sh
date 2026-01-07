#!/bin/bash

set -o xtrace   # Write all commands first to stderr
set -o errexit  # Exit the script with error if any of the commands fail

# Supported/used environment variables:
# MONGODB_URI     Set the suggested connection MONGODB_URI (including credentials and topology info)
# SPARK_VERSION   The spark version to test against
MONGODB_URI=${MONGODB_URI:-}
SPARK_VERSION=${SPARK_VERSION:-4.0.1}

export JAVA_HOME="/opt/java/jdk17"

############################################
#            Main Program                  #
############################################


echo "Running tests connecting to $MONGODB_URI on JDK${JAVA_VERSION}"

./gradlew -version
./gradlew -Dorg.mongodb.test.uri=${MONGODB_URI} --stacktrace --info integrationTest -DsparkVersion=$SPARK_VERSION
