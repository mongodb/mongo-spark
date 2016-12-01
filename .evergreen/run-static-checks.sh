#!/bin/bash

set -o xtrace   # Write all commands first to stderr
set -o errexit  # Exit the script with error if any of the commands fail

JAVA_HOME="/opt/java/jdk8"

############################################
#            Main Program                  #
############################################

echo "Running static checks"

./sbt -java-home $JAVA_HOME version
./sbt -java-home $JAVA_HOME clean compile doc scalastyle
