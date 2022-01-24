#!/bin/bash

set -o xtrace   # Write all commands first to stderr
set -o errexit  # Exit the script with error if any of the commands fail

# Supported/used environment variables:
#  JAVA_VERSION            The version of java to use for the tests
#  MONGODB_URI             Set the suggested connection MONGODB_URI (including credentials and topology info)
JAVA_VERSION=${JAVA_VERSION:-11}
MONGODB_URI=${MONGODB_URI:-}

export JDK8="/opt/java/jdk8"
export JDK11="/opt/java/jdk11"
export JAVA_HOME=$JDK11

############################################
#            Main Program                  #
############################################


echo "Running tests connecting to $MONGODB_URI on JDK${JAVA_VERSION}"

./gradlew -version
./gradlew -PjavaVersion=$JAVA_VERSION -Dorg.mongodb.test.uri=${MONGODB_URI} --stacktrace --info integrationTest
