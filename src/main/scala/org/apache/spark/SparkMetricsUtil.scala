/*
 * Copyright 2016 MongoDB, Inc.
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

package org.apache.spark

import org.apache.spark.executor.{InputMetrics, OutputMetrics}

/**
 * Object responsible for update metrics in RDD.
 * It's needed cos spark only give access to update metrics if the code is in package [org.apache.spark].
 */
object SparkMetricsUtil {

  /**
   * Increments records read metric
   * @param inputMetrics - Input Metrics Object.
   * @param value - Value to be incremented.
   */
  def incRecordsRead(inputMetrics: InputMetrics, value: Long): Unit = {
    inputMetrics.incRecordsRead(value)
  }

  /**
   * Increments bytes read metric
   * @param inputMetrics - Input Metrics Object.
   * @param value - Value to be incremented.
   */
  def incBytesRead(inputMetrics: InputMetrics, value: Long): Unit = {
    inputMetrics.incBytesRead(value)
  }

  /**
   * Sets bytes read metric
   * @param inputMetrics - Input Metrics Object.
   * @param value - Value to set.
   */
  def setBytesRead(inputMetrics: InputMetrics, value: Long): Unit = {
    inputMetrics.setBytesRead(value)
  }

  /**
   * Sets records written metric.
   * @param outputMetrics - Output Metrics Object.
   * @param value - Value to set.
   */
  def setRecordsWritten(outputMetrics: OutputMetrics, value: Long): Unit = {
    outputMetrics.setRecordsWritten(value);
  }

  /**
   * Sets bytes written metric.
   * @param outputMetrics - Output Metrics Object.
   * @param value - Value to set.
   */
  def setBytesWritten(outputMetrics: OutputMetrics, value: Long): Unit = {
    outputMetrics.setBytesWritten(value);
  }

}
