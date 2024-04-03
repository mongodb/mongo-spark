package com.mongodb.spark.sql.connector.connection;

import static com.mongodb.spark.sql.connector.config.MongoConfig.CONNECTION_STRING_CONFIG;
import static com.mongodb.spark.sql.connector.config.MongoConfig.DATABASE_NAME_CONFIG;
import static com.mongodb.spark.sql.connector.config.MongoConfig.PREFIX;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotSame;

import com.mongodb.client.MongoClient;
import com.mongodb.client.internal.MongoClientImpl;
import com.mongodb.spark.sql.connector.config.MongoConfig;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class DefaultMongoClientFactoryTest {

  private static final Map<String, String> CONFIG_MAP = new HashMap<>();

  static {
    CONFIG_MAP.put(PREFIX + CONNECTION_STRING_CONFIG, "mongodb://localhost:27017");
    CONFIG_MAP.put(PREFIX + DATABASE_NAME_CONFIG, "db");
  }

  @Test
  void factoriesWithSameConfigCreateClientsWithEqualSettings() {
    MongoConfig config = MongoConfig.createConfig(CONFIG_MAP);
    DefaultMongoClientFactory factory1 = new DefaultMongoClientFactory(config);
    DefaultMongoClientFactory factory2 = new DefaultMongoClientFactory(config);

    MongoClient client1 = factory1.create();
    MongoClient client2 = factory2.create();

    assertInstanceOf(MongoClientImpl.class, client1);
    assertInstanceOf(MongoClientImpl.class, client2);
    assertEquals(
        ((MongoClientImpl) client1).getSettings(), ((MongoClientImpl) client2).getSettings());
  }

  @Test
  void factoriesWithSameConfigCreateNotSameClients() {
    MongoConfig config = MongoConfig.createConfig(CONFIG_MAP);
    DefaultMongoClientFactory factory1 = new DefaultMongoClientFactory(config);
    DefaultMongoClientFactory factory2 = new DefaultMongoClientFactory(config);

    MongoClient client1 = factory1.create();
    MongoClient client2 = factory2.create();

    assertNotSame(client1, client2);
  }
}
