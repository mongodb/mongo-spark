package com.mongodb.spark.sql.connector.config;

import static com.mongodb.spark.sql.connector.config.MongoConfig.CONNECTION_STRING_CONFIG;
import static com.mongodb.spark.sql.connector.config.MongoConfig.DATABASE_NAME_CONFIG;
import static com.mongodb.spark.sql.connector.config.MongoConfig.PREFIX;
import static org.junit.jupiter.api.Assertions.*;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class SimpleMongoConfigTest {

  private static final Map<String, String> CONFIG_MAP = new HashMap<>();

  static {
    CONFIG_MAP.put(PREFIX + CONNECTION_STRING_CONFIG, "mongodb://localhost:27017");
    CONFIG_MAP.put(PREFIX + DATABASE_NAME_CONFIG, "db");
  }

  @Test
  void createSimpleConfig() {
    MongoConfig config = MongoConfig.createConfig(CONFIG_MAP);

    assertInstanceOf(SimpleMongoConfig.class, config);
  }

  @Test
  void configsCreateForSameMapAreEqual() {
    MongoConfig config1 = MongoConfig.createConfig(CONFIG_MAP);
    MongoConfig config2 = MongoConfig.createConfig(CONFIG_MAP);

    assertEquals(config1, config2);
  }
}
