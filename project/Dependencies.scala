/*
  * Copyright 2016 MongoDB, Inc.
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  *   http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */

import sbt._

object Dependencies {
  // Versions
  val scalaCoreVersion        = "2.11.12"
  val scalaVersions           = Seq("2.11.12", "2.12.7")
  val mongodbDriverVersion    = "3.9.0"
  val sparkVersion            = "2.4.0"
  val slf4jVersion            = "1.7.16"

  val scalaTestVersion        = "3.0.5"
  val scalaCheckVersion       = "1.14.0"
  val scalaMockVersion        = "3.6.0"
  val junitVersion            = "4.12"
  val junitInterfaceVersion   = "0.11"

  // Libraries
  val mongodbDriver     = "org.mongodb" % "mongo-java-driver" % mongodbDriverVersion
  val sparkCore         = "org.apache.spark" %% "spark-core" % sparkVersion % "provided"
  val sparkSql          = "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"
  val slf4j             = "org.slf4j" % "slf4j-api" % slf4jVersion % "provided"

  // Test
  val scalaTest         = "org.scalatest" %% "scalatest" % scalaTestVersion % "test"
  val scalaCheck        = "org.scalacheck" %% "scalacheck" % scalaCheckVersion % "test"
  val scalaMock         = "org.scalamock" %% "scalamock-scalatest-support" % scalaMockVersion % "test"
  val junit             = "junit" % "junit" % junitVersion % "test"
  val junitInterface    = "com.novocode" % "junit-interface" % junitInterfaceVersion % "test"
  val sparkStreaming    = "org.apache.spark" %% "spark-streaming" % sparkVersion % "test"

  // Projects
  val coreDependencies     = Seq(mongodbDriver, sparkCore, sparkSql, slf4j)
  val testDependencies     = Seq(scalaTest, scalaCheck, scalaMock, junit, junitInterface, sparkStreaming)

  }
