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
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class ComplexBean implements Serializable {

  private BoxedBean boxedBean;
  private List<String> listField;
  private Map<String, String> mapField;
  private List<Map<String, String>> complexListField;
  private Map<String, List<String>> complexMapField;

  public ComplexBean() {}

  public ComplexBean(
      final BoxedBean boxedBean,
      final List<String> listField,
      final Map<String, String> mapField,
      final List<Map<String, String>> complexListField,
      final Map<String, List<String>> complexMapField) {
    this.boxedBean = boxedBean;
    this.listField = listField;
    this.mapField = mapField;
    this.complexListField = complexListField;
    this.complexMapField = complexMapField;
  }

  public BoxedBean getBoxedBean() {
    return boxedBean;
  }

  public void setBoxedBean(final BoxedBean boxedBean) {
    this.boxedBean = boxedBean;
  }

  public List<String> getListField() {
    return listField;
  }

  public void setListField(final List<String> listField) {
    this.listField = listField;
  }

  public Map<String, String> getMapField() {
    return mapField;
  }

  public void setMapField(final Map<String, String> mapField) {
    this.mapField = mapField;
  }

  public List<Map<String, String>> getComplexListField() {
    return complexListField;
  }

  public void setComplexListField(final List<Map<String, String>> complexListField) {
    this.complexListField = complexListField;
  }

  public Map<String, List<String>> getComplexMapField() {
    return complexMapField;
  }

  public void setComplexMapField(final Map<String, List<String>> complexMapField) {
    this.complexMapField = complexMapField;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ComplexBean that = (ComplexBean) o;
    return Objects.equals(boxedBean, that.boxedBean)
        && Objects.equals(listField, that.listField)
        && Objects.equals(mapField, that.mapField)
        && Objects.equals(complexListField, that.complexListField)
        && Objects.equals(complexMapField, that.complexMapField);
  }

  @Override
  public int hashCode() {
    return Objects.hash(boxedBean, listField, mapField, complexListField, complexMapField);
  }

  @Override
  public String toString() {
    return "ComplexBean{" + "boxedBean="
        + boxedBean + ", listField="
        + listField + ", mapField="
        + mapField + ", complexListField="
        + complexListField + ", complexMapField="
        + complexMapField + '}';
  }
}
