#!/bin/bash

set -o xtrace   # Write all commands first to stderr
set -o errexit  # Exit the script with error if any of the commands fail

# Supported/used environment variables:
#       MONGODB_URI             Set the suggested connection MONGODB_URI (including credentials and topology info)

MONGODB_URI=${MONGODB_URI:-}
JAVA_HOME="/opt/java/jdk8"

############################################
#            Main Program                  #
############################################

echo "Running coverage"

./sbt -java-home $JAVA_HOME version
./sbt -java-home $JAVA_HOME coverage test -Dorg.mongodb.test.uri=${MONGODB_URI}
./sbt -java-home $JAVA_HOME coverageAggregate
./sbt -java-home $JAVA_HOME coverageReport
