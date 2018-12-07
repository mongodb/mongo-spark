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

import scalariform.formatter.preferences.FormattingPreferences

import com.typesafe.sbt.SbtScalariform._
import org.scalastyle.sbt.ScalastylePlugin._
import sbtsparkpackage.SparkPackagePlugin.autoImport._
import sbt.Keys._
import sbt._

object SparkConnectorBuild extends Build {

  import Dependencies.{scalaCoreVersion, scalaVersions, coreDependencies, testDependencies}
  import Resolvers._

  val buildSettings = Seq(
    organization := "org.mongodb.spark",
    organizationHomepage := Some(url("http://www.mongodb.org")),
    version := "2.4.1-SNAPSHOT",
    scalaVersion := scalaCoreVersion,
    crossScalaVersions := scalaVersions,
    libraryDependencies ++= coreDependencies,
    resolvers := mongoScalaResolvers,
    scalacOptions ++= scalacOptionsVersion(scalaVersion.value),
    javacOptions ++= Seq("-encoding", "UTF-8")
  )

  def scalacOptionsVersion(scalaVersion: String): Seq[String] = {
    val optionalFlags = CrossVersion.partialVersion(scalaVersion) match {
      case Some((2, 11)) => Seq("-Xlint:-missing-interpolator") // scalastyle:ignore
      case _ => Seq.empty
    }
    Seq("-unchecked", "-deprecation", "-feature", "-Xlint") ++ optionalFlags
  }

  /*
   * Test Settings
   */
  val testSettings = Seq(
    testFrameworks += TestFrameworks.ScalaTest,
    testOptions += Tests.Argument(TestFrameworks.JUnit, "-q", "-v"),
    parallelExecution in Test := false,
    libraryDependencies ++= testDependencies
  )

  val scoverageSettings = Seq()

  // See https://github.com/databricks/sbt-spark-package
  val sparkPackages = Seq(
    spName := "mongodb/mongo-spark",
    sparkVersion := Dependencies.sparkVersion,
    sparkComponents := Seq("sql"),
    spAppendScalaVersion := true,
    spIncludeMaven := true,
    spIgnoreProvided := true,
    licenses += "Apache-2.0" -> url("http://opensource.org/licenses/Apache-2.0"),
    credentials += Credentials(Path.userHome / ".ivy2" / ".spCredentials")
  )

  /*
   * Style and formatting
   */
  def scalariFormFormattingPreferences: FormattingPreferences = {
    import scalariform.formatter.preferences._
    FormattingPreferences()
      .setPreference(AlignParameters, true)
      .setPreference(AlignSingleLineCaseStatements, true)
      .setPreference(DoubleIndentClassDeclaration, true)
  }

  val customScalariformSettings = scalariformSettings ++ Seq(
    ScalariformKeys.preferences in Compile := scalariFormFormattingPreferences,
    ScalariformKeys.preferences in Test := scalariFormFormattingPreferences
  )

  val scalaStyleSettings = Seq(
    (scalastyleConfig in Compile) := file("project/scalastyle-config.xml"),
    (scalastyleConfig in Test) := file("project/scalastyle-config.xml")
  )

  // Check style
  val checkAlias = addCommandAlias("check", ";clean;scalastyle;coverage;test;coverageAggregate;coverageReport")

  lazy val connector = Project(
    id = "mongo-spark-connector",
    base = file(".")
  ).settings(buildSettings)
    .settings(testSettings)
    .settings(customScalariformSettings)
    .settings(scalaStyleSettings)
    .settings(scoverageSettings)
    .settings(Publish.settings)
    .settings(Publish.assemblySettings)
    .settings(sparkPackages)
    .settings(checkAlias)

  lazy val examples = Project(
    id = "mongo-spark-connector-examples",
    base = file("examples")
  ).dependsOn(connector)
    .settings(buildSettings)
    .settings(testSettings)
    .settings(customScalariformSettings)
    .settings(scalaStyleSettings)
    .settings(Publish.noPublishing)

}
