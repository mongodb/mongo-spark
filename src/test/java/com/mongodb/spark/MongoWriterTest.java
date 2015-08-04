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

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.rdd.RDD;
import org.bson.BsonMaxKey;
import org.bson.BsonMinKey;
import org.bson.Document;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import scala.reflect.ClassTag$;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;

/*
 * These tests assume a single mongod running on localhost:30000
 * with authentication available for a user with name 'test' and password 'password'
 * for the database 'test'
 */
public class MongoWriterTest {
    private String database = "spark_test";
    private String collection = "test";

    private String uri = "mongodb://spark_test:password@localhost:30000/spark_test";

    private String master = "local";
    private String appName = "testApp";

    private SparkConf sparkConf = new SparkConf().setMaster(master)
            .setAppName(appName);
    private SparkContext sc;

    private MongoClientProvider clientProvider = new MongoSparkClientProvider(uri);
    private MongoCollectionProvider<Document> collectionProvider =
            new MongoSparkCollectionProvider<>(Document.class, clientProvider, database, collection);

    private String key = "a";
    private List<Document> documents = asList(new Document(key, 0), new Document(key, 1), new Document(key, 2));

    @Before
    public void setUp() {
        MongoClient client = new MongoClient(new MongoClientURI(uri));
        client.getDatabase(database).getCollection(collection).drop();
        client.getDatabase(database).getCollection(collection).insertMany(documents);
        client.getDatabase(database).getCollection(collection).createIndex(new Document(key, 1));
        client.close();
        sc = new SparkContext(sparkConf);
    }

    @After
    public void tearDown() {
        sc.stop();
        sc = null;
    }

    @Test
    public void shouldInsertToMongo() {
        RDD<Document> mongoRdd = new MongoRDD<>(sc, collectionProvider, Document.class, key)
                .map(new GetSingleKeyValueDocument(key), ClassTag$.MODULE$.apply(Document.class));

        MongoWriter.writeToMongo(mongoRdd, collectionProvider, false, false);

        assertEquals(2 * documents.size(), collectionProvider.getCollection().count());
    }

    @Test
    public void shouldNotUpsertToMongoDueToUpsertOption() {
        RDD<Document> mongoRdd = new MongoRDD<>(sc, collectionProvider, Document.class, key);

        assertEquals(documents.size(), mongoRdd.cache().count());

        collectionProvider.getCollection()
                .deleteMany(new Document(key, new Document("$gt", new BsonMinKey()).append("$lt", new BsonMaxKey())));
        assertEquals(0, collectionProvider.getCollection().count());

        MongoWriter.writeToMongo(mongoRdd, collectionProvider, false, false);

        assertEquals(0, collectionProvider.getCollection().count());
    }

    @Test
    public void shouldUpsertToMongoDueToUpsertOption() {
        RDD<Document> mongoRdd = new MongoRDD<>(sc, collectionProvider, Document.class, key)
                .map(new GetSingleKeyValueDocument(key), ClassTag$.MODULE$.apply(Document.class));

        assertEquals(documents.size(), mongoRdd.cache().count());

        collectionProvider.getCollection()
                .deleteMany(new Document(key, new Document("$gt", new BsonMinKey()).append("$lt", new BsonMaxKey())));
        assertEquals(0, collectionProvider.getCollection().count());

        MongoWriter.writeToMongo(mongoRdd, collectionProvider, true, false);

        assertEquals(documents.size(), collectionProvider.getCollection().count());
        assertNotSame(documents, collectionProvider.getCollection().find().into(new ArrayList<>()));
        List<Document> docs = new ArrayList<>();
        Collections.addAll(docs, (Document[]) mongoRdd.collect());
        assertEquals(docs, collectionProvider.getCollection().find().into(new ArrayList<>()));
    }

    @Test
    public void shouldNotUpsertToMongoDocumentsAlreadyExist() {
        RDD<Document> mongoRdd = new MongoRDD<>(sc, collectionProvider, Document.class, key);

        MongoWriter.writeToMongo(mongoRdd, collectionProvider, true, false);

        assertEquals(documents.size(), collectionProvider.getCollection().count());
        assertEquals(documents, collectionProvider.getCollection().find().into(new ArrayList<>()));
    }

    @Test
    public void shouldReplaceInMongo() {
        RDD<Document> mongoRdd = new MongoRDD<>(sc, collectionProvider, Document.class, key)
                .map(new ChangeNonIDKeyValue(), ClassTag$.MODULE$.apply(Document.class));

        MongoWriter.writeToMongo(mongoRdd, collectionProvider, false, false);

        assertEquals(documents.size(), collectionProvider.getCollection().count());
        assertNotSame(documents, collectionProvider.getCollection().find().into(new ArrayList<>()));
        List<Document> docs = new ArrayList<>();
        Collections.addAll(docs, (Document[]) mongoRdd.collect());
        assertEquals(docs, collectionProvider.getCollection().find().into(new ArrayList<>()));
    }

    @Test
    public void shouldNotReplaceInMongo() {
        RDD<Document> mongoRdd = new MongoRDD<>(sc, collectionProvider, Document.class, key)
                .map(new ChangeIDValue(), ClassTag$.MODULE$.apply(Document.class));

        MongoWriter.writeToMongo(mongoRdd, collectionProvider, false, false);
        assertEquals(documents.size(), collectionProvider.getCollection().count());
        assertEquals(documents, collectionProvider.getCollection().find().into(new ArrayList<>()));
    }

    @Test
    public void shouldCreateAndWriteToCollection() {
        RDD<Document> mongoRdd = new MongoRDD<>(sc, collectionProvider, Document.class, key)
                .map(new GetSingleKeyValueDocument(key), ClassTag$.MODULE$.apply(Document.class));

        MongoCollectionProvider<Document> docProvider =
                new MongoSparkCollectionProvider<>(Document.class, clientProvider, database, "spark_test_empty");
        docProvider.getCollection().drop();
        assertEquals(0, docProvider.getCollection().count());
        MongoWriter.writeToMongo(mongoRdd, docProvider, true, false);
        assertEquals(documents.size(), docProvider.getCollection().count());
    }
}

// essentially removes _id from each document - JavaRDD can use lambdas instead of serializable abstract functions
class GetSingleKeyValueDocument extends SerializableAbstractFunction1<Document, Document> {
    private String key;

    public GetSingleKeyValueDocument(final String key) {
        this.key = key;
    }

    @Override
    public Document apply(final Document document) {
        return new Document(key, document.get(key));
    }
}

class ChangeNonIDKeyValue extends SerializableAbstractFunction1<Document, Document> {
    @Override
    public Document apply(final Document document) {
        return new Document("_id", document.getObjectId("_id")).append("b", 0);
    }
}

class ChangeIDValue extends SerializableAbstractFunction1<Document, Document> {
    @Override
    public Document apply(final Document document) {
        return new Document("_id", 0).append("a", document.get("a"));
    }
}
