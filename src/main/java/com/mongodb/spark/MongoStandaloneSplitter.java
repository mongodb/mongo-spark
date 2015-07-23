/*
 * Copyright (c) 2008-2015 MongoDB, Inc.
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

import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonMaxKey;
import org.bson.BsonMinKey;
import org.bson.BsonString;
import org.bson.BsonValue;

/**
 * A splitter for standalone mongo.
 */
public class MongoStandaloneSplitter implements MongoSplitter {
    private MongoCollectionFactory factory;

    /**
     * Constructs a new instance
     *
     * @param factory a mongo collection factory
     * @param <T> the type of documents in the collection
     */
    public <T> MongoStandaloneSplitter(final MongoCollectionFactory<T> factory) {
        this.factory = factory;
    }

    // db.adminCommand({splitVector: "test.test", keyPattern: {"_id": 1}, maxChunkSize: 1, force: false})
    @Override
    public BsonValue[] getSplitKeys(final int maxChunkSize) {
        BsonDocument result =
                this.factory.getDatabase().runCommand(
                        new BsonDocument("splitVector", new BsonString(this.factory.getCollection().getNamespace().getFullName()))
                                 .append("keyPattern", new BsonDocument("_id", new BsonInt32(1)))
                                 .append("maxChunkSize", new BsonInt32(maxChunkSize)),
                        BsonDocument.class);

        BsonArray splitKeyValues;

        if (result.get("ok").equals(new BsonDouble(1.0))) {
            splitKeyValues = (BsonArray) result.get("splitKeys");
        }
        else {
            // something went wrong
            // return null or min/maxkey?
            return new BsonValue[] {new BsonMinKey(), new BsonMaxKey()};
        }

        int numSplitKeys = splitKeyValues.size();
        BsonValue[] splitKeys = new BsonValue[1 + numSplitKeys + 1];

        splitKeys[0] = new BsonMinKey();
        int i;
        for (i = 0; i < numSplitKeys; i++) {
            splitKeys[i + 1] = splitKeyValues.get(i).asDocument().get("_id");
        }
        splitKeys[i + 1] = new BsonMaxKey();

        return splitKeys;
    }
}
