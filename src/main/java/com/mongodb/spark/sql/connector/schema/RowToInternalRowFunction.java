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

package com.mongodb.spark.sql.connector.schema;

import static com.mongodb.spark.sql.connector.schema.ConverterHelper.SCHEMA_TO_EXPRESSION_ENCODER_FUNCTION;

import java.io.Serializable;
import java.util.function.Function;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.types.StructType;

/**
 * A Row to InternalRow function that uses a resolved and bound encoder for the given schema.
 *
 * <p>A concrete {@code Function} implementation that is {@code Serializable}, so it can be
 * serialized and sent to executors.
 */
final class RowToInternalRowFunction implements Function<Row, InternalRow>, Serializable {
  private static final long serialVersionUID = 1L;

  private final ExpressionEncoder.Serializer<Row> serializer;

  RowToInternalRowFunction(final StructType schema) {
    serializer = SCHEMA_TO_EXPRESSION_ENCODER_FUNCTION.apply(schema).createSerializer();
  }

  @Override
  public InternalRow apply(final Row row) {
    return serializer.apply(row);
  }
}
