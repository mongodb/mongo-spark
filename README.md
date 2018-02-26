# MongoDB Spark Connector 

The official MongoDB Spark Connector.

[![Build Status](https://travis-ci.org/mongodb/mongo-spark.svg?branch=master)]
(https://travis-ci.org/mongodb/mongo-spark)

## Documentation

See: https://docs.mongodb.com/spark-connector/

[API Documentation](https://www.javadoc.io/doc/org.mongodb.spark/mongo-spark-connector_2.11)

See the [1.x](https://github.com/mongodb/mongo-spark/tree/1.x) branch for the Spark 1.6.x version of the driver.

## Downloading

The connector is published on [Spark packages](https://spark-packages.org/package/mongodb/mongo-spark), the community index of third-party packages for [Apache Spark](http://spark.apache.org/). The binaries and dependency information for Maven, SBT, Ivy, and others can also be found on
[Maven Central](http://search.maven.org/#search%7Cga%7C1%7Cg%3Aorg.mongodb.spark).

## Support / Feedback

For issues with, questions about, or feedback for the MongoDB Spark Connector, please look into
our [support channels](https://docs.mongodb.com/manual/support/). Please
do not email any of the developers directly with issues or
questions - you're more likely to get an answer on the [mongodb-user](https://groups.google.com/group/mongodb-user) discussion forum.

At a minimum, please include in your description the exact version of the driver that you are using.  If you are having
connectivity issues, it's often also useful to paste in the Spark configuration. You should also check your application logs for
any connectivity-related exceptions and post those as well.

## Bugs / Feature Requests

Think you’ve found a bug? Want to see a new feature in the Spark driver? Please open a case in our issue management tool, JIRA:

- [Create an account and login](https://jira.mongodb.org).
- Navigate to [the SPARK project](https://jira.mongodb.org/browse/SPARK).
- Click **Create Issue** - Please provide as much information as possible about the issue type and how to reproduce it.

Bug reports in JIRA for the driver and the Core Server (i.e. SERVER) project are **public**.

If you’ve identified a security vulnerability in a driver or any other MongoDB project, please report it according to the
[instructions here](https://docs.mongodb.com/manual/tutorial/create-a-vulnerability-report/).

## Versioning

Major increments (such as 1.x -> 2.x) will occur when break changes are being made to the public API.  All methods and
classes removed in a major release will have been deprecated in a prior release of the previous major release branch, and/or otherwise
called out in the release notes.

Minor 1.x increments (such as 1.1, 1.2, etc) will occur when non-trivial new functionality is added or significant enhancements or bug
fixes occur that may have behavioral changes that may affect some edge cases (such as dependence on behavior resulting from a bug). An
example of an enhancement is a method or class added to support new functionality added to the MongoDB server.   Minor releases will
almost always be binary compatible with prior minor releases from the same major release branch, exept as noted below.

Patch 1.x.y increments (such as 1.0.0 -> 1.0.1, 1.1.1 -> 1.1.2, etc) will occur for bug fixes only and will always be binary compatible
with prior patch releases of the same minor release branch.


## Build

### Note: The following instructions are intended for internal use. 
#### Please see the [downloading](#downloading) instructions for information on getting and using the MongoDB Spark Connector.

To build the driver:

```
$ git clone https://github.com/mongodb/mongo-spark.git
$ cd mongo-spark
$ ./sbt check
```

To publish the signed jars:

```
$ ./sbt +publish-signed
```

To publish to spark packages:

```
$ ./sbt +spPublish
```
See the [sbt-spark-package](https://github.com/databricks/sbt-spark-package) plugin for more information.

## Maintainers

* Ross Lawley          ross@mongodb.com

Additional contributors can be found [here](https://github.com/mongodb/mongo-spark/graphs/contributors).
