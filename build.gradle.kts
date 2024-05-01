/*
 * Copyright 2008-present MongoDB, Inc.
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

import java.io.ByteArrayOutputStream
import java.net.URI

buildscript {
    repositories {
        mavenCentral()
    }
}

plugins {
    idea
    `java-library`
    `maven-publish`
    signing
    checkstyle
    id("com.github.gmazzo.buildconfig") version "3.0.2"
    id("com.github.spotbugs") version "4.7.9"
    id("com.diffplug.spotless") version "6.19.0"
    id("com.github.johnrengelman.shadow") version "7.0.0"
}

version = "10.3.0"
group = "org.mongodb.spark"

description = "The official MongoDB Apache Spark Connect Connector."

java {
    sourceCompatibility = JavaVersion.VERSION_1_8
    targetCompatibility = JavaVersion.VERSION_1_8
}

repositories {
    mavenCentral()
}

// Usage: ./gradlew -DscalaVersion=2.12 -DsparkVersion=3.1.4
val scalaVersion = System.getProperty("scalaVersion", "2.13")
val sparkVersion = System.getProperty("sparkVersion", "3.5.1")

extra.apply {
    set("annotationsVersion", "22.0.0")
    set("mongodbDriverVersion", "[4.8.1,4.8.99)")
    set("sparkVersion", sparkVersion)
    set("scalaVersion", scalaVersion)

    // Testing dependencies
    set("junitJupiterVersion", "5.7.2")
    set("junitPlatformVersion", "1.7.2")
    set("mockitoVersion", "3.12.4")

    // Integration test dependencies
    set("commons-lang3", "3.12.0")
}

sourceSets {
    main {
        java {
            val scalaInteropSrcDir = if (scalaVersion == "2.12") "java_scala_212" else "java_scala_213"
            srcDirs("src/main/java", "src/main/$scalaInteropSrcDir")
        }
    }
}

dependencies {
    compileOnly("org.jetbrains:annotations:${project.extra["annotationsVersion"]}")

    implementation("org.mongodb:mongodb-driver-sync:${project.extra["mongodbDriverVersion"]}")

    compileOnly("org.apache.spark:spark-core_$scalaVersion:$sparkVersion")
    compileOnly("org.apache.spark:spark-sql_$scalaVersion:$sparkVersion")
    compileOnly("org.apache.spark:spark-catalyst_$scalaVersion:$sparkVersion")
    compileOnly("org.apache.spark:spark-streaming_$scalaVersion:$sparkVersion")

    shadow("org.mongodb:mongodb-driver-sync:${project.extra["mongodbDriverVersion"]}")

    // Test version of Spark
    testImplementation("org.apache.spark:spark-core_$scalaVersion:$sparkVersion")
    testImplementation("org.apache.spark:spark-sql_$scalaVersion:$sparkVersion")
    testImplementation("org.apache.spark:spark-catalyst_$scalaVersion:$sparkVersion")
    testImplementation("org.apache.spark:spark-streaming_$scalaVersion:$sparkVersion")

    // Unit Tests
    testImplementation(platform("org.junit:junit-bom:5.8.1"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("org.mockito:mockito-junit-jupiter:${project.extra["mockitoVersion"]}")
    testImplementation("org.apiguardian:apiguardian-api:1.1.2") // https://github.com/gradle/gradle/issues/18627

    // Integration Tests
    testImplementation("org.apache.commons:commons-lang3:${project.extra["commons-lang3"]}")
}

val defaultJdkVersion: Int = 11

java {
    toolchain.languageVersion.set(JavaLanguageVersion.of(defaultJdkVersion))
}

tasks.withType<JavaCompile> {
    options.encoding = "UTF-8"
    options.release.set(8)
}

// ===========================
//     Build Config
// ===========================
val gitVersion: String by lazy {
    val describeStdOut = ByteArrayOutputStream()
    exec {
        commandLine = listOf("git", "describe", "--tags", "--always", "--dirty")
        standardOutput = describeStdOut
    }
    describeStdOut.toString().substring(1).trim()
}

val gitDiffNameOnly: String by lazy {
    val describeStdOut = ByteArrayOutputStream()
    exec {
        commandLine = listOf("git", "diff", "--name-only")
        standardOutput = describeStdOut
    }
    describeStdOut.toString().replaceIndent(" - ")
}

buildConfig {
    className("Versions")
    packageName("com.mongodb.spark.connector")
    useJavaOutput()
    buildConfigField("String", "NAME", "\"mongo-spark-connector\"")
    buildConfigField("String", "VERSION", provider { "\"$gitVersion\"" })
}

// ===========================
//     Testing
// ===========================
sourceSets.create("integrationTest") {
    java.srcDir("src/integrationTest/java")
    compileClasspath += sourceSets["main"].output + configurations["testRuntimeClasspath"]
    runtimeClasspath += output + compileClasspath + sourceSets["test"].runtimeClasspath
}

tasks.create("integrationTest", Test::class.java) {
    description = "Runs the integration tests"
    group = "verification"
    testClassesDirs = sourceSets["integrationTest"].output.classesDirs
    classpath = sourceSets["integrationTest"].runtimeClasspath
    outputs.upToDateWhen { false }
    mustRunAfter("test")
}

// Configure tests
tasks.withType<Test> {
    tasks.getByName("check").dependsOn(this)
    useJUnitPlatform()
    testLogging {
        events("passed", "skipped", "failed")
    }

    val javaVersion: Int = (project.findProperty("javaVersion") as String? ?: defaultJdkVersion.toString()).toInt()
    logger.info("Running tests using JDK$javaVersion")
    javaLauncher.set(
        javaToolchains.launcherFor {
            languageVersion.set(JavaLanguageVersion.of(javaVersion))
        },
    )

    systemProperties(mapOf("org.mongodb.test.uri" to System.getProperty("org.mongodb.test.uri", "")))

    val jdkHome = project.findProperty("jdkHome") as String?
    jdkHome.let {
        val javaExecutablesPath = File(jdkHome, "bin/java")
        if (javaExecutablesPath.exists()) {
            executable = javaExecutablesPath.absolutePath
        }
    }

    addTestListener(object : TestListener {
        override fun beforeTest(testDescriptor: TestDescriptor?) {}
        override fun beforeSuite(suite: TestDescriptor?) {}
        override fun afterTest(testDescriptor: TestDescriptor?, result: TestResult?) {}
        override fun afterSuite(d: TestDescriptor?, r: TestResult?) {
            if (d != null && r != null && d.parent == null) {
                val resultsSummary = """Tests summary:
                    | Scala Version: $scalaVersion,
                    | Spark Version: $sparkVersion,
                    | ${r.testCount} tests,
                    | ${r.successfulTestCount} succeeded,
                    | ${r.failedTestCount} failed,
                    | ${r.skippedTestCount} skipped
                """.trimMargin().replace("\n", "")

                val border = "=".repeat(resultsSummary.length)
                logger.lifecycle("\n$border")
                logger.lifecycle("Test result: ${r.resultType}")
                logger.lifecycle(resultsSummary)
                logger.lifecycle("${border}\n")
            }
        }
    })
}

// ===========================
//     Code Quality checks
// ===========================
checkstyle {
    toolVersion = "7.4"
}

spotbugs {
    excludeFilter.set(project.file("config/spotbugs/exclude.xml"))
    showProgress.set(true)
    setReportLevel("high")
    setEffort("max")
}

tasks.withType<com.github.spotbugs.snom.SpotBugsTask> {
    enabled = baseName.equals("main")
    reports.maybeCreate("html").isEnabled = !project.hasProperty("xmlReports.enabled")
    reports.maybeCreate("xml").isEnabled = project.hasProperty("xmlReports.enabled")
}

// Spotless is used to lint and reformat source files.
spotless {
    java {
        importOrder("java", "io", "org", "org.bson", "com.mongodb", "com.mongodb.spark", "")
        removeUnusedImports() // removes any unused imports
        trimTrailingWhitespace()
        endWithNewline()
        indentWithSpaces()

        palantirJavaFormat().style("GOOGLE")
    }

    kotlinGradle {
        ktlint()
        trimTrailingWhitespace()
        indentWithSpaces()
        endWithNewline()
    }

    format("extraneous") {
        target("*.xml", "*.yml", "*.md")
        trimTrailingWhitespace()
        indentWithSpaces()
        endWithNewline()
    }
}

// Auto apply spotless on compile
tasks.named("compileJava") {
    dependsOn(":spotlessApply")
}

// ===========================
//       Publishing
// ===========================
tasks.shadowJar {
    configurations = listOf(project.configurations.shadow.get())
}

tasks.register<Jar>("sourcesJar") {
    description = "Create the sources jar"
    from(sourceSets.main.get().allSource)
    archiveClassifier.set("sources")
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
}

tasks.register<Jar>("javadocJar") {
    description = "Create the Javadoc jar"
    from(tasks.javadoc)
    archiveClassifier.set("javadoc")
}

publishing {
    publications {
        create<MavenPublication>("mavenJava") {
            artifactId = "mongo-spark-connector_$scalaVersion"
            from(components["java"])
            artifact(tasks["sourcesJar"])
            artifact(tasks["javadocJar"])

            pom {
                name.set(project.name)
                description.set(project.description)
                url.set("http://www.mongodb.org")
                licenses {
                    license {
                        name.set("The Apache License, Version 2.0")
                        url.set("http://www.apache.org/licenses/LICENSE-2.0.txt")
                    }
                }
                developers {
                    developer {
                        id.set("Various")
                        organization.set("MongoDB")
                    }
                }
                scm {
                    connection.set("scm:https://github.com/mongodb/mongo-spark.git")
                    developerConnection.set("scm:git@github.com:mongodb/mongo-spark.git")
                    url.set("https://github.com/mongodb/mongo-spark")
                }
            }
        }
    }

    repositories {
        maven {
            val snapshotsRepoUrl = URI("https://oss.sonatype.org/content/repositories/snapshots/")
            val releasesRepoUrl = URI("https://oss.sonatype.org/service/local/staging/deploy/maven2/")
            url = if (version.toString().endsWith("SNAPSHOT")) snapshotsRepoUrl else releasesRepoUrl
            credentials {
                val nexusUsername: String? by project
                val nexusPassword: String? by project
                username = nexusUsername ?: ""
                password = nexusPassword ?: ""
            }
        }
    }
}

signing {
    val signingKey: String? by project
    val signingPassword: String? by project
    useInMemoryPgpKeys(signingKey, signingPassword)
    sign(publishing.publications["mavenJava"])
}

tasks.javadoc {
    val doclet = options as StandardJavadocDocletOptions
    if (JavaVersion.current().isJava9Compatible) {
        doclet.addBooleanOption("html5", true)
    }
    doclet.links("http://docs.oracle.com/javase/8/docs/api/")
    doclet.links("https://spark.apache.org/docs/latest/api/java/")
}

tasks.register("publishSnapshots") {
    group = "publishing"
    description = "Publishes snapshots to Sonatype"
    if (version.toString().endsWith("-SNAPSHOT")) {
        dependsOn("publish")
    }
}

tasks.register("publishArchives") {
    group = "publishing"
    description = "Publishes a release and uploads to Sonatype / Maven Central"

    doFirst {
        if (gitVersion != version) {
            val cause = """
                | Version mismatch:
                | =================
                |
                | $version != $gitVersion
                |
                | Modified Files:
                |$gitDiffNameOnly
                |
                | The project version does not match the git tag.
                |
            """.trimMargin()
            throw GradleException(cause)
        } else {
            println("Publishing: ${project.name} : $gitVersion")
        }
    }

    if (gitVersion == version) {
        dependsOn("publish")
    }
}
