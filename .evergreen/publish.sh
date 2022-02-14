#!/bin/bash

# DO NOT ECHO COMMANDS AS THEY CONTAIN SECRETS!

set -o errexit  # Exit the script with error if any of the commands fail

############################################
#            Main Program                  #
############################################

echo "Publishing"

export JDK11="/opt/java/jdk11"
export JAVA_HOME=$JDK11

RELEASE=${RELEASE:false}

export ORG_GRADLE_PROJECT_nexusUsername=${NEXUS_USERNAME}
export ORG_GRADLE_PROJECT_nexusPassword=${NEXUS_PASSWORD}
export ORG_GRADLE_PROJECT_signingKey="${SIGNING_KEY}"
export ORG_GRADLE_PROJECT_signingPassword=${SIGNING_PASSWORD}

if [ "$RELEASE" == "true" ]; then
  TASK="publishArchives"
else
  TASK="publishSnapshots"
fi

SYSTEM_PROPERTIES="-Dorg.gradle.internal.publish.checksums.insecure=true -Dorg.gradle.internal.http.connectionTimeout=120000 -Dorg.gradle.internal.http.socketTimeout=120000"

./gradlew -version
./gradlew ${SYSTEM_PROPERTIES} --stacktrace --info  ${TASK}
