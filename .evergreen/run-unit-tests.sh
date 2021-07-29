#!/bin/bash

set -o xtrace   # Write all commands first to stderr
set -o errexit  # Exit the script with error if any of the commands fail

# Supported/used environment variables:
#       JDK                     Set the version of java to be used.  Java versions can be set from the java toolchain /opt/java "jdk8"

JDK=${JDK:-jdk}
export JAVA_HOME="/opt/java/${JDK}"

############################################
#            Main Program                  #
############################################


echo "Running tests with ${JDK} connecting to $MONGODB_URI"

./gradlew -version
./gradlew --stacktrace --info test
