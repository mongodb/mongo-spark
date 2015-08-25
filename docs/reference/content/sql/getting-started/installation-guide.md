+++
date = "2015-08-25T11:13:17-04:00"
draft = true
title = "Installation Guide"
[menu.main]
  parent = "SQL Getting Started"
  identifier = "SQL Installation Guide"
  weight = 10
  pre = "<i class='fa'></i>"
+++

# Installation

There is a single Mavin artifact in the 1.0-SNAPSHOT release. The artifact
is the uber-jar `mongo-spark-sql`. The recommend way to get started using the
connector in your project is with a dependency management system.

{{< distroPicker >}}

## Uber Mongo Spark SQL Connector

An uber jar containing the core and Spark SQL connector library,
mongo-java-driver, spark-core, and spark-sql.

{{< install artifactId="mongo-spark-sql" version="1.0-SNAPSHOT">}}
