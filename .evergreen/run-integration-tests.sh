#!/bin/bash

set -o xtrace   # Write all commands first to stderr
set -o errexit  # Exit the script with error if any of the commands fail

# Supported/used environment variables:
#       MONGODB_URI             Set the suggested connection MONGODB_URI (including credentials and topology info)
#       JDK                     Set the version of java to be used.  Java versions can be set from the java toolchain /opt/java "jdk8"

MONGODB_URI=${MONGODB_URI:-}
JDK=${JDK:-jdk}
export JAVA_HOME="/opt/java/${JDK}"

############################################
#            Main Program                  #
############################################


echo "Running tests with ${JDK} connecting to $MONGODB_URI"

./gradlew -version
./gradlew -Dorg.mongodb.test.uri=${MONGODB_URI} --stacktrace --info integrationTest
