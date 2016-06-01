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

package com.mongodb.spark.sql.fieldTypes.api.java;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.JavaRequiresMongoDB;
import org.apache.spark.api.java.JavaSparkContext;
import org.bson.BsonBinary;
import org.bson.BsonBinarySubType;
import org.bson.BsonDbPointer;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonJavaScript;
import org.bson.BsonJavaScriptWithScope;
import org.bson.BsonMaxKey;
import org.bson.BsonMinKey;
import org.bson.BsonObjectId;
import org.bson.BsonRegularExpression;
import org.bson.BsonSymbol;
import org.bson.BsonTimestamp;
import org.bson.BsonUndefined;
import org.bson.Document;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public final class FieldTypesTest extends JavaRequiresMongoDB {

    private BsonObjectId bsonObjectId = new BsonObjectId(new org.bson.types.ObjectId());
    private BsonBinary bsonBinary = new BsonBinary(BsonBinarySubType.OLD_BINARY, new byte[]{1, 1, 1, 1, 1});
    private BsonDbPointer bsonDbPointer = new BsonDbPointer("db.coll", bsonObjectId.getValue());
    private BsonJavaScript bsonJavaScript = new BsonJavaScript("int i = 0;");
    private BsonJavaScriptWithScope bsonJavaScriptWithScope = new BsonJavaScriptWithScope("int x = y", new BsonDocument("y", new BsonInt32(1)));
    private BsonMaxKey bsonMaxKey = new BsonMaxKey();
    private BsonMinKey bsonMinKey = new BsonMinKey();
    private BsonRegularExpression bsonRegularExpression = new BsonRegularExpression("^test.*regex.*xyz$", "i");
    private BsonSymbol bsonSymbol = new BsonSymbol("ruby stuff");
    private BsonTimestamp bsonTimestamp = new BsonTimestamp(0x12345678, 5);
    private BsonUndefined bsonUndefined = new BsonUndefined();
    private BsonDocument getBsonDocument() {
        BsonDocument document = new BsonDocument();
        document.put("_id", bsonObjectId);
        document.put("binary", bsonBinary);
        document.put("dbPointer", bsonDbPointer);
        document.put("javaScript", bsonJavaScript);
        document.put("javaScriptWithScope", bsonJavaScriptWithScope);
        document.put("maxKey", bsonMaxKey);
        document.put("minKey", bsonMinKey);
        document.put("regularExpression", bsonRegularExpression);
        document.put("symbol", bsonSymbol);
        document.put("timestamp", bsonTimestamp);
        document.put("undefined", bsonUndefined);
        return document;
    }

    @Test
    public void shouldAllowTheRoundTrippingOfAJavaBeanRepresentingComplexBsonTypes() {
        // Given
        String newCollectionName = getCollectionName() + "_new";
        JavaSparkContext jsc = getJavaSparkContext();
        List<Document> original = new ArrayList<Document>();
        List<Document> copied = new ArrayList<Document>();

        getDatabase().getCollection(getCollectionName(), BsonDocument.class).insertOne(getBsonDocument());

        // When
        MongoSpark.load(jsc).toDF()
                .write().format("com.mongodb.spark.sql").option("collection", newCollectionName).option("mode", "overwrite").save();

        getDatabase().getCollection(getCollectionName()).find().into(original);
        getDatabase().getCollection(newCollectionName).find().into(copied);

        // Then
        assertThat(original, is(copied));
    }

    @Test
    public void shouldBeAbleToCreateADatasetBasedOnAJavaBeanRepresentingComplexBsonTypes(){
        // Given
        JavaSparkContext jsc = getJavaSparkContext();
        getDatabase().getCollection(getCollectionName(), BsonDocument.class).insertOne(getBsonDocument());

        BsonTypesJavaBean expected = new BsonTypesJavaBean(new ObjectId(bsonObjectId.getValue()),
                new Binary(bsonBinary.getType(), bsonBinary.getData()), new DbPointer(bsonDbPointer.getNamespace(), bsonDbPointer.getId()),
                new JavaScript(bsonJavaScript.getCode()),
                new JavaScriptWithScope(bsonJavaScriptWithScope.getCode(), bsonJavaScriptWithScope.getScope()), new MinKey(), new MaxKey(),
                new RegularExpression(bsonRegularExpression.getPattern(), bsonRegularExpression.getOptions()),
                new Symbol(bsonSymbol.getSymbol()), new Timestamp(bsonTimestamp.getTime(), bsonTimestamp.getInc()), new Undefined());

        // When
        BsonTypesJavaBean loaded = MongoSpark.load(jsc).toDS(BsonTypesJavaBean.class).first();

        // Then
        assertEquals(loaded, expected);
    }

}
