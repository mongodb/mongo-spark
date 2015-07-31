# mongo-spark

## Purpose

mongo-spark is a library that allows users to integrate
[MongoDB](https://www.mongodb.com/)
into the
[Apache Spark](http://spark.apache.org/)
workflow.

mongo-spark provides a connecter for Spark which allows MongoDB to be used as
an input source for RDDs as well as providing options to write RDDs to MongoDB.

## Features
* Use MongoDB collections from standalone, replica set, or sharded
  configurations as input for RDDs.
* Write RDDs to MongoDB collections.
* Filter source data with aggregation.

## Building

Run `./gradlew jar` to build the jar file. The jar will be placed in the
`build/libs` directory.

## Usage

Please see the
[mongo-spark Wiki](https://github.com/mongodbinc-interns/mongo-spark/wiki)
for example usage.
