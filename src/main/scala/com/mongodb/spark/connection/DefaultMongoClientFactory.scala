/*
 * Copyright 2016 MongoDB, Inc.
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

package com.mongodb.spark.connection

import com.mongodb.spark.MongoClientFactory
import com.mongodb.{ConnectionString, MongoClient, MongoClientURI, ServerAddress}

private[spark] case class DefaultMongoClientFactory(connectionString: String) extends MongoClientFactory {
  @transient val parsedConnectionString = new ConnectionString(connectionString)

  override def create(): MongoClient = new MongoClient(new MongoClientURI(connectionString))
}
