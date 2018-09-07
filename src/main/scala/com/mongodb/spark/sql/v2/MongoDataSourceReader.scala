/*
 * Copyright 2018 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.spark.sql.v2

import java.util

import com.mongodb.MongoClient
import com.mongodb.client.{AggregateIterable, MongoCursor}
import com.mongodb.spark.config.ReadConfig
import com.mongodb.spark.rdd.partitioner.MongoPartition
import com.mongodb.spark.sql.MapFunctions.documentToRow
import com.mongodb.spark.sql.{MongoInferSchema, MongoRelationHelper}
import com.mongodb.spark.{LoggingTrait, MongoConnector, MongoSpark}
import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.v2.reader.{DataReader, DataReaderFactory, DataSourceReader, SupportsPushDownFilters, SupportsPushDownRequiredColumns}
import org.apache.spark.sql.sources.{Filter, IsNotNull}
import org.apache.spark.sql.types.StructType
import org.bson.BsonDocument
import org.bson.conversions.Bson

import scala.collection.JavaConverters._
import scala.collection.mutable

case class MongoDataSourceReader(private val schemaOption: Option[StructType], private val readConfig: ReadConfig) extends DataSourceReader
    with SupportsPushDownFilters with SupportsPushDownRequiredColumns {
  private lazy val schema: StructType = schemaOption.getOrElse(MongoInferSchema(MongoSpark.load[BsonDocument](SparkContext.getOrCreate(), readConfig)))
  private lazy val pipeline: Seq[BsonDocument] = readConfig.aggregationConfig.pipeline.getOrElse(Nil)
  private val filters = mutable.ListBuffer.empty[Filter]
  private var requiredSchema: Option[StructType] = None

  override def readSchema(): StructType = requiredSchema.getOrElse(schema)

  override def createDataReaderFactories(): util.List[DataReaderFactory[Row]] = {
    val connector = MongoConnector(readConfig)
    val partitions = readConfig.partitioner.partitions(connector, readConfig, Array())
    partitions.map(p => MongoDataReaderFactory(connector, p, filters.toArray, requiredSchema).asInstanceOf[DataReaderFactory[Row]]).toList.asJava
  }

  override def pushFilters(pushedFilters: Array[Filter]): Array[Filter] = {
    filters ++= pushedFilters.toSeq
    filters.toArray
  }

  override def pushedFilters(): Array[Filter] = filters.toArray

  override def pruneColumns(schema: StructType): Unit = requiredSchema = Some(schema)

  private case class MongoDataReaderFactory(connector: MongoConnector, partition: MongoPartition, filters: Array[Filter],
                                            requiredSchema: Option[StructType]) extends DataReaderFactory[Row] with LoggingTrait {
    override def createDataReader(): DataReader[Row] = {
      MongoDataReader(connector, partition, createPipeline())
    }

    override def preferredLocations(): Array[String] = partition.locations.toArray

    private def createPipeline(): Seq[Bson] = {
      if (readConfig.pipelineIncludeFiltersAndProjections) {
        val pipelineFilters = if (readConfig.pipelineIncludeNullFilters) {
          schema.fields.filter(!_.nullable).map(_.name).map(IsNotNull) ++ filters
        } else {
          filters
        }
        val requiredColumns = requiredSchema.map(_.fieldNames).getOrElse(Array.empty[String])
        if (requiredColumns.nonEmpty || filters.nonEmpty) {
          logInfo(s"requiredColumns: ${requiredColumns.mkString(", ")}, filters: ${pipelineFilters.mkString(", ")}")
        }

        pipeline ++ MongoRelationHelper.createPipeline(requiredColumns, pipelineFilters)
      } else {
        logInfo(s"${ReadConfig.pipelineIncludeFiltersAndProjectionsProperty} set to false. Filters and Projections have not been pushed to MongoDB")
        pipeline
      }
    }
  }

  private case class MongoDataReader(connector: MongoConnector, partition: MongoPartition, pipeline: Seq[Bson]) extends DataReader[Row] {
    var _mongoClient: Option[MongoClient] = None
    var _cursor: Option[MongoCursor[BsonDocument]] = None

    def mongoClient: MongoClient = {
      if (_mongoClient.isEmpty) {
        _mongoClient = Some(connector.acquireClient())
      }
      _mongoClient.get
    }

    def cursor: MongoCursor[BsonDocument] = {
      if (_cursor.isEmpty) {

        val partitionPipeline: Seq[Bson] = if (partition.queryBounds.isEmpty) {
          pipeline
        } else {
          new BsonDocument("$match", partition.queryBounds) +: pipeline
        }

        val aggregateIterable: AggregateIterable[BsonDocument] = mongoClient.getDatabase(readConfig.databaseName)
          .getCollection[BsonDocument](readConfig.collectionName, classOf[BsonDocument])
          .withReadConcern(readConfig.readConcern)
          .withReadPreference(readConfig.readPreference)
          .aggregate(partitionPipeline.asJava)
          .allowDiskUse(readConfig.aggregationConfig.allowDiskUse)

        readConfig.aggregationConfig.hint.map(aggregateIterable.hint)
        readConfig.aggregationConfig.collation.map(aggregateIterable.collation)
        _cursor = Some(aggregateIterable.iterator)
      }
      _cursor.get
    }

    override def next(): Boolean = cursor.hasNext

    override def get(): Row = documentToRow(cursor.next(), readSchema())

    override def close(): Unit = {
      _cursor.foreach(_.close())
      _mongoClient.foreach(connector.releaseClient)
    }
  }
}

