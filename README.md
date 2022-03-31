# MongoDB Spark Connector

The official MongoDB Spark Connector.

## Documentation

See: https://docs.mongodb.com/spark-connector/

[API Documentation](https://www.javadoc.io/doc/org.mongodb.spark/mongo-spark-connector)

## Downloading

The binaries and dependency information for Maven, SBT, Ivy, and others can also be found on
[Maven Central](https://search.maven.org/#search?q=g:org.mongodb.spark).

## Support / Feedback

For issues with, questions about, or feedback for the MongoDB Kafka Connector, please look into our
[support channels](http://www.mongodb.org/about/support). Please do not email any of the Kafka connector developers directly with issues or
questions - you're more likely to get an answer on the
[MongoDB Community Forums](https://community.mongodb.com/tags/c/drivers-odms-connectors/7/spark-connector).

At a minimum, please include in your description the exact version of the driver that you are using.  If you are having
connectivity issues, it's often also useful to paste in the Kafka connector configuration. You should also check your application logs for
any connectivity-related exceptions and post those as well.

## Bugs / Feature Requests

Think you’ve found a bug? Want to see a new feature in the Kafka driver? Please open a case in our issue management tool, JIRA:

- [Create an account and login](https://jira.mongodb.org).
- Navigate to [the SPARK project](https://jira.mongodb.org/browse/SPARK).
- Click **Create Issue** - Please provide as much information as possible about the issue type and how to reproduce it.

Bug reports in JIRA for the connector are **public**.

If you’ve identified a security vulnerability in a connector or any other MongoDB project, please report it according to the
[instructions here](https://docs.mongodb.com/manual/tutorial/create-a-vulnerability-report/).


## Build

### Note: The following instructions are intended for internal use.
#### Please see the [downloading](#downloading) instructions for information on getting and using the MongoDB Spark Connector.

To build the connector:

```
$ git clone https://github.com/mongodb/mongo-spark.git
$ cd mongo-spark
$ ./gradlew clean check
```
