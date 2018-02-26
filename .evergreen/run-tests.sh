#!/bin/bash

set -o xtrace   # Write all commands first to stderr
set -o errexit  # Exit the script with error if any of the commands fail

# Supported/used environment variables:
#       AUTH                    Set to enable authentication. Values are: "auth" / "noauth" (default)
#       SSL                     Set to enable SSL. Values are "ssl" / "nossl" (default)
#       MONGODB_URI             Set the suggested connection MONGODB_URI (including credentials and topology info)
#       TOPOLOGY                Allows you to modify variables and the MONGODB_URI based on test topology
#                               Supported values: "server", "replica_set", "sharded_cluster"
#       SCALA_VERSION           Set the version of Scala to be used.


AUTH=${AUTH:-noauth}
SSL=${SSL:-nossl}
MONGODB_URI=${MONGODB_URI:-}
TOPOLOGY=${TOPOLOGY:-server}

############################################
#            Functions                     #
############################################

provision_ssl () {
  echo "SSL !"

  # We generate the keystore and truststore on every run with the certs in the drivers-tools repo
  if [ ! -f client.pkc ]; then
    openssl pkcs12 -CAfile ${DRIVERS_TOOLS}/.evergreen/x509gen/ca.pem -export -in ${DRIVERS_TOOLS}/.evergreen/x509gen/client.pem -out client.pkc -password pass:bithere
  fi
  if [ ! -f mongo-truststore ]; then
    echo "y" | ${JAVA_HOME}/bin/keytool -importcert -trustcacerts -file ${DRIVERS_TOOLS}/.evergreen/x509gen/ca.pem -keystore mongo-truststore -storepass hithere
  fi

  # We add extra java options arguments for SSL
  export _JAVA_OPTIONS=" -Djavax.net.ssl.keyStoreType=pkcs12 -Djavax.net.ssl.keyStore=`pwd`/client.pkc -Djavax.net.ssl.keyStorePassword=bithere -Djavax.net.ssl.trustStoreType=jks -Djavax.net.ssl.trustStore=`pwd`/mongo-truststore -Djavax.net.ssl.trustStorePassword=hithere"

  # Arguments for auth + SSL
  if [ "$AUTH" != "noauth" ] || [ "$TOPOLOGY" == "replica_set" ]; then
    export MONGODB_URI="${MONGODB_URI}&ssl=true"
  else
    export MONGODB_URI="${MONGODB_URI}/?ssl=true"
  fi
}

############################################
#            Main Program                  #
############################################
JAVA_HOME="/opt/java/jdk8"

# Provision the correct connection string and set up SSL if needed
if [ "$TOPOLOGY" == "sharded_cluster" ]; then

     if [ "$AUTH" = "auth" ]; then
       export MONGODB_URI="mongodb://bob:pwd123@localhost:27017/?authSource=admin"
     else
       export MONGODB_URI="mongodb://localhost:27017"
     fi
fi

echo "Running Scala $SCALA_VERSION test with $AUTH over $SSL for $TOPOLOGY and connecting to $MONGODB_URI"


./sbt -java-home $JAVA_HOME version

if [ "$SSL" != "nossl" ]; then
    ./sbt -java-home $JAVA_HOME ++${SCALA_VERSION} compile
    provision_ssl
fi

./sbt -java-home $JAVA_HOME ++${SCALA_VERSION} test -Dorg.mongodb.test.uri=${MONGODB_URI}
