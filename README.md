# mongo-spark

## Purpose

mongo-spark is a library that allows users to integrate
[MongoDB](https://www.mongodb.com/)
into the
[Apache Spark](http://spark.apache.org/)
workflow.

mongo-spark provides a connector for Spark which allows MongoDB to be used as
an input source for RDDs as well as providing options to write RDDs to MongoDB.

It also provides integration with Spark SQL, including schema inference and
document to row mapping.

## Features

* Use MongoDB collections from standalone, replica set, or sharded
  configurations as input for RDDs.
* Filter source data with aggregation.
* Write RDDs to MongoDB collections.
* Infer Spark SQL schemas for Mongo collections.
* Map documents to Spark SQL rows.

## Building

Run `./gradlew jar` to build the jars. Jars will be place in the build/libs
directory of each module. For example, the `core` jar will be placed in
`core/build/libs`.

## Usage

Please see the
[mongo-spark Wiki](https://github.com/mongodbinc-interns/mongo-spark/wiki)
for example usage.
