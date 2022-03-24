/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.mongodb.spark.sql.connector.config;

import static java.lang.String.format;
import static java.util.Collections.singletonList;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.mongodb.WriteConcern;

import com.mongodb.spark.sql.connector.exceptions.ConfigException;

/**
 * The Write Configuration
 *
 * <p>The {@link MongoConfig} for writes
 */
public final class WriteConfig extends AbstractMongoConfig {
  private static final long serialVersionUID = 1L;

  /** The operation type for the write. */
  public enum OperationType {
    /** Insert the whole document */
    INSERT("insert"),
    /** Replace the whole document or insert a new one if it doesn't exist */
    REPLACE("replace"),
    /** Update the document or insert a new one if it doesn't exist */
    UPDATE("update");

    private final String value;

    OperationType(final String operationType) {
      this.value = operationType;
    }

    static OperationType fromString(final String operationType) {
      for (OperationType writeOperationType : OperationType.values()) {
        if (operationType.equalsIgnoreCase(writeOperationType.value)) {
          return writeOperationType;
        }
      }
      throw new ConfigException(format("'%s' is not a valid Write Operation Type", operationType));
    }

    @Override
    public String toString() {
      return value;
    }
  }

  /**
   * The maximum batch size for the batch in the bulk operation.
   *
   * <p>Configuration: {@value}
   *
   * <p>Default: {@value MAX_BATCH_SIZE_DEFAULT}
   */
  public static final String MAX_BATCH_SIZE_CONFIG = "maxBatchSize";

  private static final int MAX_BATCH_SIZE_DEFAULT = 512;

  /**
   * Use ordered bulk operations
   *
   * <p>Configuration: {@value}
   *
   * <p>Default: {@value ORDERED_BULK_OPERATION_DEFAULT}
   */
  public static final String ORDERED_BULK_OPERATION_CONFIG = "ordered";

  private static final boolean ORDERED_BULK_OPERATION_DEFAULT = true;

  /**
   * The write operation type to perform
   *
   * <p>The options are:
   *
   * <ul>
   *   <li>insert: Inserts the data.
   *   <li>replace: Replaces an existing document that matches the {@link #ID_FIELD_CONFIG} or
   *       inserts the data if no match.
   *   <li>update: Updates an existing document that matches the {@link #ID_FIELD_CONFIG} with the
   *       new data or inserts the data if no match.
   * </ul>
   *
   * <p>Configuration: {@value}
   *
   * <p>Default: "replace"
   */
  public static final String OPERATION_TYPE_CONFIG = "operationType";

  private static final OperationType OPERATION_TYPE_DEFAULT = OperationType.REPLACE;

  /**
   * A comma delimited field list used to identify a document
   *
   * <p>Configuration: {@value}
   *
   * <p>Default: "[_id]".
   *
   * <p>Note: For sharded clusters use the shard key.
   */
  public static final String ID_FIELD_CONFIG = "idFieldList";

  private static final List<String> ID_FIELD_DEFAULT = singletonList("_id");

  /**
   * The optional {@link WriteConcern} w property.
   *
   * <p>Users can use the name of the write concern eg: {@code MAJORITY}, {@code W1} or they can
   * provide the number of MongoDB's required to acknowledge the write before continuing.
   *
   * <p>Configuration: {@value}
   *
   * <p>Note: The default write concern is {@code WriteConcern.ACKNOWLEDGED}.
   */
  public static final String WRITE_CONCERN_W_CONFIG = "writeConcern.w";

  /**
   * The optional {@link WriteConcern} journal property.
   *
   * <p>Configuration: {@value}
   *
   * <p>Note: Must be a boolean string: {@code true} or {@code false}.
   */
  public static final String WRITE_CONCERN_JOURNAL_CONFIG = "writeConcern.journal";

  /**
   * The optional {@link WriteConcern} wTimeout property in milliseconds.
   *
   * <p>Configuration: {@value}
   *
   * <p>Note: Must be a valid integer
   */
  public static final String WRITE_CONCERN_W_TIMEOUT_MS_CONFIG = "writeConcern.wTimeoutMS";

  private final WriteConcern writeConcern;
  private final OperationType operationType;

  /**
   * Construct a new instance
   *
   * @param options the options for configuration
   */
  WriteConfig(final Map<String, String> options) {
    super(options, UsageMode.WRITE);
    writeConcern = createWriteConcern();
    operationType =
        OperationType.fromString(getOrDefault(OPERATION_TYPE_CONFIG, OPERATION_TYPE_DEFAULT.value));
  }

  @Override
  public WriteConfig withOption(final String key, final String value) {
    Map<String, String> options = new HashMap<>();
    options.put(key, value);
    return withOptions(options);
  }

  @Override
  public WriteConfig withOptions(final Map<String, String> options) {
    if (options.isEmpty()) {
      return this;
    }
    return new WriteConfig(withOverrides(WRITE_PREFIX, options));
  }

  /** @return the max size of bulk operation batches */
  public int getMaxBatchSize() {
    return getInt(MAX_BATCH_SIZE_CONFIG, MAX_BATCH_SIZE_DEFAULT);
  }

  /** @return the operation type */
  public OperationType getOperationType() {
    return operationType;
  }

  /** @return the write concern to sue */
  public WriteConcern getWriteConcern() {
    return writeConcern;
  }

  /** @return the field list used to identify the document */
  public List<String> getIdFields() {
    return getList(ID_FIELD_CONFIG, ID_FIELD_DEFAULT);
  }

  /** @return true if the bulk operation is ordered */
  public boolean isOrdered() {
    return getBoolean(ORDERED_BULK_OPERATION_CONFIG, ORDERED_BULK_OPERATION_DEFAULT);
  }

  private WriteConcern createWriteConcern() {
    WriteConcern writeConcern = WriteConcern.ACKNOWLEDGED;
    try {
      if (containsKey(WRITE_CONCERN_W_CONFIG)) {
        try {
          writeConcern = writeConcern.withW(getInt(WRITE_CONCERN_W_CONFIG, -1));
        } catch (NumberFormatException e) {
          // ignore
          writeConcern = writeConcern.withW(get(WRITE_CONCERN_W_CONFIG));
        }
      }

      if (containsKey(WRITE_CONCERN_JOURNAL_CONFIG)) {
        writeConcern = writeConcern.withJournal(getBoolean(WRITE_CONCERN_JOURNAL_CONFIG, false));
      }

      if (containsKey(WRITE_CONCERN_W_TIMEOUT_MS_CONFIG)) {
        writeConcern =
            writeConcern.withWTimeout(
                getInt(WRITE_CONCERN_W_TIMEOUT_MS_CONFIG, -1), TimeUnit.MILLISECONDS);
      }
    } catch (RuntimeException e) {
      throw new ConfigException("Invalid write concern configuration.", e);
    }
    return writeConcern;
  }
}
