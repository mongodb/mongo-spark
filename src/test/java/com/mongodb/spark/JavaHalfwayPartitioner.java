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

package com.mongodb.spark;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Projections;
import com.mongodb.spark.config.ReadConfig;
import com.mongodb.spark.rdd.partitioner.MongoPartition;
import com.mongodb.spark.rdd.partitioner.MongoPartitioner;
import org.apache.spark.api.java.function.Function;
import org.bson.BsonDocument;
import org.bson.BsonValue;

public final class JavaHalfwayPartitioner implements MongoPartitioner {

    @Override
    public MongoPartition[] partitions(final MongoConnector connector, final ReadConfig readConfig, final BsonDocument[] pipeline) {
        BsonValue midId = connector.withCollectionDo(readConfig, BsonDocument.class, new Function<MongoCollection<BsonDocument>,
                BsonValue>() {
            @Override
            public BsonValue call(final MongoCollection<BsonDocument> collection) throws Exception {
                int midPoint = (int) (collection.countDocuments() / 2);
                return collection.find().skip(midPoint).limit(1).projection(Projections.include("_id")).first().get("_id");
            }
        });
        return new MongoPartition[]{ MongoPartition.create(0, new BsonDocument("_id", new BsonDocument("$lt", midId))),
                                     MongoPartition.create(1, new BsonDocument("_id", new BsonDocument("$gte", midId)))};
    }

}
