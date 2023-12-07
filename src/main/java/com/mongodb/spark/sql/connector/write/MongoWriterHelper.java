package com.mongodb.spark.sql.connector.write;

import java.util.Map;

import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import com.mongodb.spark.sql.connector.MongoCatalog;

/** Created by Alban on 07 Dec, 2023 */
public class MongoWriterHelper {

  public void createTable(String database, Map<String, String> options) {
    MongoCatalog catalog = new MongoCatalog();
    catalog.initialize(database, new CaseInsensitiveStringMap(options));

    catalog.createTable();
  }

  public void dropTable(String database, Map<String, String> options) {
    dropTable(database, null, options);
  }

  public void dropTable(String database, String collection, Map<String, String> options) {
    MongoCatalog catalog = new MongoCatalog();
    catalog.initialize(database, new CaseInsensitiveStringMap(options));

    catalog.dropTable(collection);
  }

  public void renameTable(String database, Map<String, String> options, String from, String to) {
    MongoCatalog catalog = new MongoCatalog();
    catalog.initialize(database, new CaseInsensitiveStringMap(options));

    try {
      catalog.renameTable(from, to);
    } catch (TableAlreadyExistsException | NoSuchTableException e) {
      throw new RuntimeException(e);
    }
  }
}
