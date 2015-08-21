# mongo-spark

## Purpose

mongo-spark is a library that allows users to integrate
[MongoDB](https://www.mongodb.com/)
into the
[Apache Spark](http://spark.apache.org/)
workflow.

mongo-spark provides a connector for Spark which allows MongoDB to be used as
an input source for RDDs as well as providing options to write RDDs to MongoDB.

## Features
* Use MongoDB collections from standalone, replica set, or sharded
  configurations as input for RDDs.
* Write RDDs to MongoDB collections.
* Filter source data with aggregation.

## Building

Run `./gradlew jar` to build the jars. Jars will be place in the build/libs
directory of each module. For example, the `core` jar will be placed in
`core/build/libs`.

## Usage

Please see the
[mongo-spark Wiki](https://github.com/mongodbinc-interns/mongo-spark/wiki)
for example usage.
