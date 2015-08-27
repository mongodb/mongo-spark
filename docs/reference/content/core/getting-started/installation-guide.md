+++
date = "2015-08-25T11:12:55-04:00"
draft = true
title = "Installation Guide"
[menu.main]
  parent = "Core Getting Started"
  identifier = "Core Installation Guide"
  weight = 10
  pre = "<i class='fa'></i>"
+++

# Installation

There are two Maven artifacts available in the 1.0-SNAPSHOT release. The
preferred artifact for applications is the `mongo-spark-core`; however, the
uber-jar `mongo-spark` is still published. The recommended way to get started
using the connector in your project is with a dependency management system.

{{< distroPicker >}}

## Mongo Spark Core Connector

The core connector includes functionality to read from and write to MongoDB.

{{% note class="important" %}}
The Scala 2.10 distributions of Spark are assumed for Apache Spark
dependencies.
{{% /note %}}

{{< install artifactId="mongo-spark-core" version="1.0-SNAPSHOT" dependencies="true">}}

## Uber Mongo Spark Connector

An uber-jar containing the core connector library, mongo-java-driver, and
spark-core.

{{< install artifactId="mongo-spark" version="1.0-SNAPSHOT">}}
