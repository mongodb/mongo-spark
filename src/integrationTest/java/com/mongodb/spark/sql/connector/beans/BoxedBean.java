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

public class BoxedBean implements Serializable {

  private Byte byteField;
  private Short shortField;
  private Integer intField;
  private Long longField;
  private Float floatField;
  private Double doubleField;
  private Boolean booleanField;

  public BoxedBean() {}

  public BoxedBean(
      final Byte byteField,
      final Short shortField,
      final Integer intField,
      final Long longField,
      final Float floatField,
      final Double doubleField,
      final Boolean booleanField) {
    this.byteField = byteField;
    this.shortField = shortField;
    this.intField = intField;
    this.longField = longField;
    this.floatField = floatField;
    this.doubleField = doubleField;
    this.booleanField = booleanField;
  }

  public Byte getByteField() {
    return byteField;
  }

  public void setByteField(final Byte byteField) {
    this.byteField = byteField;
  }

  public Short getShortField() {
    return shortField;
  }

  public void setShortField(final Short shortField) {
    this.shortField = shortField;
  }

  public Integer getIntField() {
    return intField;
  }

  public void setIntField(final Integer intField) {
    this.intField = intField;
  }

  public Long getLongField() {
    return longField;
  }

  public void setLongField(final Long longField) {
    this.longField = longField;
  }

  public Float getFloatField() {
    return floatField;
  }

  public void setFloatField(final Float floatField) {
    this.floatField = floatField;
  }

  public Double getDoubleField() {
    return doubleField;
  }

  public void setDoubleField(final Double doubleField) {
    this.doubleField = doubleField;
  }

  public Boolean getBooleanField() {
    return booleanField;
  }

  public void setBooleanField(final Boolean booleanField) {
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
    BoxedBean boxedBean = (BoxedBean) o;
    return Objects.equals(byteField, boxedBean.byteField)
        && Objects.equals(shortField, boxedBean.shortField)
        && Objects.equals(intField, boxedBean.intField)
        && Objects.equals(longField, boxedBean.longField)
        && Objects.equals(floatField, boxedBean.floatField)
        && Objects.equals(doubleField, boxedBean.doubleField)
        && Objects.equals(booleanField, boxedBean.booleanField);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        byteField, shortField, intField, longField, floatField, doubleField, booleanField);
  }

  @Override
  public String toString() {
    return "BoxedBean{" + "byteField="
        + byteField + ", shortField="
        + shortField + ", intField="
        + intField + ", longField="
        + longField + ", floatField="
        + floatField + ", doubleField="
        + doubleField + ", booleanField="
        + booleanField + '}';
  }
}
