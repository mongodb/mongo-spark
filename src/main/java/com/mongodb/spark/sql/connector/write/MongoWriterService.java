package com.mongodb.spark.sql.connector.write;

import java.util.Map;

import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import com.mongodb.spark.sql.connector.MongoCatalog;

/** Created by Alban on 07 Dec, 2023 */
public class MongoWriterService {

  MongoCatalog mongoCatalog = new MongoCatalog();

  public MongoWriterService(String database, Map<String, String> options) {
    mongoCatalog.initialize(database, new CaseInsensitiveStringMap(options));
  }

  public void createTable() {
    mongoCatalog.createTable();
  }

  public void dropTable() {
    dropTable(null);
  }

  public void dropTable(String collection) {
    mongoCatalog.dropTable(collection);
  }

  public void renameTable(String from, String to) {
    try {
      mongoCatalog.renameTable(from, to);
    } catch (TableAlreadyExistsException | NoSuchTableException e) {
      throw new RuntimeException(e);
    }
  }

  public boolean tableExists(String collection) {
    return mongoCatalog.tableExists(collection);
  }
}
