#!/bin/bash

set -o xtrace   # Write all commands first to stderr
set -o errexit  # Exit the script with error if any of the commands fail

export JAVA_HOME="/opt/java/jdk8"

############################################
#            Main Program                  #
############################################

echo "Compiling and running checks"

# We always compile with the latest version of java
./gradlew -version
./gradlew -PxmlReports.enabled=true --info -x test -x integrationTest clean check jar testClasses javadoc
