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

import com.mongodb.spark.sql.connector.annotations.ThreadSafe;
import com.mongodb.spark.sql.connector.exceptions.MongoSparkException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.function.Function;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.analysis.SimpleAnalyzer$;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.types.StructType;
import scala.collection.immutable.Seq;

@ThreadSafe
class SchemaToExpressionEncoderFunction implements Function<StructType, ExpressionEncoder<Row>> {
  private static final Object MODULE_STATIC;
  private static final Method APPLY_METHOD;
  private static final Function<StructType, Seq<Attribute>> ATTRIBUTE_FUNCTION;

  static {
    Class<?> clazz;
    Object moduleStatic = null;
    Method applyMethod = null;
    Function<StructType, Seq<Attribute>> attributeFunction = null;

    try {
      // Test Spark 3.1 - 3.4 support
      // Get method to convert schema -> rowEncoder
      clazz = Class.forName("org.apache.spark.sql.catalyst.encoders.RowEncoder$");
      moduleStatic = clazz.getDeclaredField("MODULE$").get(null);
      applyMethod = clazz.getMethod("apply", StructType.class);

      // Get method to convert schema -> Seq<Attribute>
      Method toAttributesMethod = StructType.class.getMethod("toAttributes");
      attributeFunction = schema -> MethodInvoker.invoke(toAttributesMethod, schema);
    } catch (ClassNotFoundException
        | NoSuchFieldException
        | NoSuchMethodException
        | IllegalAccessException e) {
      // Ignore - not available in this version of Spark
    }

    if (attributeFunction == null) {
      try {
        // Test Spark 3.5 support
        // Get method to convert schema -> rowEncoder
        clazz = Class.forName("org.apache.spark.sql.catalyst.encoders.ExpressionEncoder$");
        moduleStatic = clazz.getDeclaredField("MODULE$").get(null);
        applyMethod = clazz.getMethod("apply", StructType.class);

        // Get method to convert schema -> Seq<Attribute>
        Object dataTypeStatic = Class.forName("org.apache.spark.sql.catalyst.types.DataTypeUtils$")
            .getDeclaredField("MODULE$")
            .get(null);
        Method toAttributesMethod =
            dataTypeStatic.getClass().getMethod("toAttributes", StructType.class);
        attributeFunction =
            schema -> MethodInvoker.invoke(toAttributesMethod, dataTypeStatic, schema);
      } catch (ClassNotFoundException
          | NoSuchFieldException
          | NoSuchMethodException
          | IllegalAccessException e) {
        // Ignore - not available in this version of Spark
      }
    }

    if (attributeFunction == null) {
      String sparkVersion = org.apache.spark.package$.MODULE$.SPARK_VERSION_SHORT();
      throw new ExceptionInInitializerError("Unsupported version of Spark: " + sparkVersion);
    }

    MODULE_STATIC = moduleStatic;
    APPLY_METHOD = applyMethod;
    ATTRIBUTE_FUNCTION = attributeFunction;
  }

  @Override
  public ExpressionEncoder<Row> apply(final StructType schema) {
    ExpressionEncoder<Row> rowEncoder = MethodInvoker.invoke(APPLY_METHOD, MODULE_STATIC, schema);
    return rowEncoder.resolveAndBind(ATTRIBUTE_FUNCTION.apply(schema), SimpleAnalyzer$.MODULE$);
  }

  static class MethodInvoker {
    /**
     * Cleanly wrap exceptions from calling `Method.invoke`
     */
    @SuppressWarnings("unchecked")
    static <T> T invoke(final Method method, final Object obj, final Object... args) {
      try {
        return (T) method.invoke(obj, args);
      } catch (InvocationTargetException e) {
        Throwable targetException = e.getTargetException();
        if (targetException instanceof RuntimeException) {
          throw (RuntimeException) targetException;
        }
        throw new MongoSparkException(targetException);
      } catch (IllegalAccessException e) {
        throw new MongoSparkException(e);
      }
    }
  }
}
