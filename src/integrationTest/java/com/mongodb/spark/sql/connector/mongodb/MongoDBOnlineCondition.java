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

package com.mongodb.spark.sql.connector.mongodb;

import static org.junit.jupiter.api.extension.ConditionEvaluationResult.disabled;
import static org.junit.jupiter.api.extension.ConditionEvaluationResult.enabled;
import static org.junit.platform.commons.support.AnnotationSupport.findAnnotation;

import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionContext;

public class MongoDBOnlineCondition implements ExecutionCondition {

  private Boolean isMongoDBOnline = null;

  private boolean isOnline() {
    if (isMongoDBOnline == null) {
      isMongoDBOnline = new MongoSparkConnectorHelper().isOnline();
    }
    return isMongoDBOnline;
  }

  private static final ConditionEvaluationResult DEFAULT =
      enabled(MongoDBOnline.class + " is not present");

  @Override
  public ConditionEvaluationResult evaluateExecutionCondition(final ExtensionContext context) {
    return context
        .getElement()
        .flatMap(annotatedElement -> findAnnotation(annotatedElement, MongoDBOnline.class))
        .map(i -> isOnline()
            ? enabled("Enabled MongoDB is online")
            : disabled("Disabled MongoDB is offline"))
        .orElse(DEFAULT);
  }
}
