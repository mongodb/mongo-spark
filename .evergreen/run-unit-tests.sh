#!/bin/bash

set -o xtrace   # Write all commands first to stderr
set -o errexit  # Exit the script with error if any of the commands fail

# Supported/used environment variables:
# SPARK_VERSION   The spark version to test against
SPARK_VERSION=${SPARK_VERSION:-4.0.1}

export JAVA_HOME="/opt/java/jdk17"

############################################
#            Main Program                  #
############################################

echo "Running unit tests on JDK${JAVA_VERSION}"

./gradlew -version
./gradlew --stacktrace --info test -DsparkVersion=$SPARK_VERSION
