package com.mongodb.spark.sql.connector.read.partitioner;

import java.util.function.Function;
import org.bson.BsonDocument;
import org.bson.BsonValue;

/**
 * Allows loading values from nested documents if the partition field is a dotted path.
 * For example, if the partition field is `a.b.c`, this will attempt to load the value from the nested document `a.b` and return the value of `c`.
 * If the path does not exist or if any part of the path is an array, it will fall back to the original behavior of loading the value from the top-level document using the full partition field as the key.
 */
class ValueLoader implements Function<BsonDocument, BsonValue> {
  private static final char DOT = '.';
  private static final String DOT_REGEX = "\\.";
  private final String[] path;
  private final String partitionField;

  ValueLoader(final String partitionField) {
    this.partitionField = partitionField;
    this.path = getPartitionField(partitionField);
  }

  /**
   * partitionField.split("\\.") should switch to fastpath non-regex split.
   */
  private static String[] getPartitionField(String partitionField) {
    try {
      return partitionField.indexOf(DOT) > 0
          ? partitionField.split(DOT_REGEX)
          : new String[] {partitionField};
    } catch (Exception e) {
      return new String[] {partitionField};
    }
  }

  @Override
  public BsonValue apply(final BsonDocument bsonDocument) {
    BsonDocument currentDocument = bsonDocument;
    for (int i = 0; i < path.length; i++) {
      String field = path[i];
      if (!currentDocument.containsKey(field)) {
        break;
      }
      BsonValue value = currentDocument.get(field);
      // If we hit an array at any point, we cannot traverse further; fall back.
      if (value.isArray()) {
        break;
      }
      // If this is the last segment of the path, return the value directly.
      if (i == path.length - 1) {
        return value;
      }
      // For non-terminal segments, we must traverse into nested documents.
      if (value.isDocument()) {
        currentDocument = value.asDocument();
      } else {
        // Non-document encountered before the last segment; fall back.
        break;
      }
    }
    // fall back to the original behavior: look up the full partition field at the top level
    return bsonDocument.get(partitionField);
  }
}
