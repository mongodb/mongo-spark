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

package com.mongodb.spark.sql;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.JavaRequiresMongoDB;
import org.apache.spark.api.java.JavaSparkContext;
import org.bson.BsonArray;
import org.bson.BsonBinary;
import org.bson.BsonBinarySubType;
import org.bson.BsonBoolean;
import org.bson.BsonDateTime;
import org.bson.BsonDbPointer;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonJavaScript;
import org.bson.BsonJavaScriptWithScope;
import org.bson.BsonMaxKey;
import org.bson.BsonMinKey;
import org.bson.BsonNull;
import org.bson.BsonObjectId;
import org.bson.BsonRegularExpression;
import org.bson.BsonString;
import org.bson.BsonSymbol;
import org.bson.BsonTimestamp;
import org.bson.BsonUndefined;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static java.util.Arrays.asList;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public final class MongoDataFrameTest extends JavaRequiresMongoDB {

    @Test
    public void shouldRoundTripAllBsonTypes() {
        // Given
        String newCollectionName = getCollectionName() + "_new";
        JavaSparkContext jsc = getJavaSparkContext();
        List<Document> original = new ArrayList<Document>();
        List<Document> copied = new ArrayList<Document>();

        BsonDocument document = new BsonDocument();
        document.put("null", new BsonNull());
        document.put("int32", new BsonInt32(42));
        document.put("int64", new BsonInt64(52L));
        document.put("boolean", new BsonBoolean(true));
        document.put("date", new BsonDateTime(new Date().getTime()));
        document.put("double", new BsonDouble(62.0));
        document.put("string", new BsonString("the fox ..."));
        document.put("minKey", new BsonMinKey());
        document.put("maxKey", new BsonMaxKey());
        document.put("objectId", new BsonObjectId(new ObjectId()));
        document.put("code", new BsonJavaScript("int i = 0;"));
        document.put("codeWithScope", new BsonJavaScriptWithScope("int x = y", new BsonDocument("y", new BsonInt32(1))));
        document.put("regex", new BsonRegularExpression("^test.*regex.*xyz$", "i"));
        document.put("symbol", new BsonSymbol("ruby stuff"));
        document.put("timestamp", new BsonTimestamp(0x12345678, 5));
        document.put("undefined", new BsonUndefined());
        document.put("binary", new BsonBinary(new byte[]{5, 4, 3, 2, 1}));
        document.put("oldBinary", new BsonBinary(BsonBinarySubType.OLD_BINARY, new byte[]{1, 1, 1, 1, 1}));
        document.put("arrayInt", new BsonArray(asList(new BsonInt32(1), new BsonInt32(2), new BsonInt32(3))));
        document.put("document", new BsonDocument("a", new BsonInt32(1)));
        document.put("dbPointer", new BsonDbPointer("db.coll", new ObjectId()));

        getDatabase().getCollection(getCollectionName(), BsonDocument.class).insertOne(document);

        // When
        MongoSpark.load(jsc).toDF()
                .write().format("com.mongodb.spark.sql").option("collection", newCollectionName).option("mode", "overwrite").save();

        getDatabase().getCollection(getCollectionName()).find().into(original);
        getDatabase().getCollection(newCollectionName).find().into(copied);

        // Then
        assertThat(original, is(copied));
    }

}
