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
package com.mongodb.spark.sql.connector.beans;

import java.io.Serializable;
import java.util.Objects;

public class PrimitiveBean implements Serializable {

  private byte byteField;
  private short shortField;
  private int intField;
  private long longField;
  private float floatField;
  private double doubleField;
  private boolean booleanField;

  public PrimitiveBean() {}

  public PrimitiveBean(
      final byte byteField,
      final short shortField,
      final int intField,
      final long longField,
      final float floatField,
      final double doubleField,
      final boolean booleanField) {
    this.byteField = byteField;
    this.shortField = shortField;
    this.intField = intField;
    this.longField = longField;
    this.floatField = floatField;
    this.doubleField = doubleField;
    this.booleanField = booleanField;
  }

  public byte getByteField() {
    return byteField;
  }

  public void setByteField(final byte byteField) {
    this.byteField = byteField;
  }

  public short getShortField() {
    return shortField;
  }

  public void setShortField(final short shortField) {
    this.shortField = shortField;
  }

  public int getIntField() {
    return intField;
  }

  public void setIntField(final int intField) {
    this.intField = intField;
  }

  public long getLongField() {
    return longField;
  }

  public void setLongField(final long longField) {
    this.longField = longField;
  }

  public float getFloatField() {
    return floatField;
  }

  public void setFloatField(final float floatField) {
    this.floatField = floatField;
  }

  public double getDoubleField() {
    return doubleField;
  }

  public void setDoubleField(final double doubleField) {
    this.doubleField = doubleField;
  }

  public boolean isBooleanField() {
    return booleanField;
  }

  public void setBooleanField(final boolean booleanField) {
    this.booleanField = booleanField;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    PrimitiveBean that = (PrimitiveBean) o;
    return byteField == that.byteField
        && shortField == that.shortField
        && intField == that.intField
        && longField == that.longField
        && Float.compare(that.floatField, floatField) == 0
        && Double.compare(that.doubleField, doubleField) == 0
        && booleanField == that.booleanField;
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        byteField, shortField, intField, longField, floatField, doubleField, booleanField);
  }

  @Override
  public String toString() {
    return "PrimitiveBean{" + "byteField="
        + byteField + ", shortField="
        + shortField + ", intField="
        + intField + ", longField="
        + longField + ", floatField="
        + floatField + ", doubleField="
        + doubleField + ", booleanField="
        + booleanField + '}';
  }
}
