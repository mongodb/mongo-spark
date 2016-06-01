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

package com.mongodb.spark.sql.fieldTypes.api.java;

public final class BsonTypesJavaBean {
    private ObjectId _id;
    private Binary binary;
    private DbPointer dbPointer;
    private JavaScript javaScript;
    private JavaScriptWithScope javaScriptWithScope;
    private MinKey minKey;
    private MaxKey maxKey;
    private RegularExpression regularExpression;
    private Symbol symbol;
    private Timestamp timestamp;
    private Undefined undefined;

    public BsonTypesJavaBean() {
    }

    public BsonTypesJavaBean(final ObjectId _id, final Binary binary, final DbPointer dbPointer, final JavaScript javaScript,
                             final JavaScriptWithScope javaScriptWithScope, final MinKey minKey, final MaxKey maxKey,
                             final RegularExpression regularExpression, final Symbol symbol, final Timestamp timestamp,
                             final Undefined undefined) {
        this._id = _id;
        this.binary = binary;
        this.dbPointer = dbPointer;
        this.javaScript = javaScript;
        this.javaScriptWithScope = javaScriptWithScope;
        this.minKey = minKey;
        this.maxKey = maxKey;
        this.regularExpression = regularExpression;
        this.symbol = symbol;
        this.timestamp = timestamp;
        this.undefined = undefined;
    }

    public ObjectId get_id() {
        return _id;
    }

    public void set_id(final ObjectId _id) {
        this._id = _id;
    }

    public Binary getBinary() {
        return binary;
    }

    public void setBinary(final Binary binary) {
        this.binary = binary;
    }

    public DbPointer getDbPointer() {
        return dbPointer;
    }

    public void setDbPointer(final DbPointer dbPointer) {
        this.dbPointer = dbPointer;
    }

    public JavaScript getJavaScript() {
        return javaScript;
    }

    public void setJavaScript(final JavaScript javaScript) {
        this.javaScript = javaScript;
    }

    public JavaScriptWithScope getJavaScriptWithScope() {
        return javaScriptWithScope;
    }

    public void setJavaScriptWithScope(final JavaScriptWithScope javaScriptWithScope) {
        this.javaScriptWithScope = javaScriptWithScope;
    }

    public MinKey getMinKey() {
        return minKey;
    }

    public void setMinKey(final MinKey minKey) {
        this.minKey = minKey;
    }

    public MaxKey getMaxKey() {
        return maxKey;
    }

    public void setMaxKey(final MaxKey maxKey) {
        this.maxKey = maxKey;
    }

    public RegularExpression getRegularExpression() {
        return regularExpression;
    }

    public void setRegularExpression(final RegularExpression regularExpression) {
        this.regularExpression = regularExpression;
    }

    public Symbol getSymbol() {
        return symbol;
    }

    public void setSymbol(final Symbol symbol) {
        this.symbol = symbol;
    }

    public Timestamp getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(final Timestamp timestamp) {
        this.timestamp = timestamp;
    }

    public Undefined getUndefined() {
        return undefined;
    }

    public void setUndefined(final Undefined undefined) {
        this.undefined = undefined;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        BsonTypesJavaBean that = (BsonTypesJavaBean) o;

        if (_id != null ? !_id.equals(that._id) : that._id != null) return false;
        if (binary != null ? !binary.equals(that.binary) : that.binary != null) return false;
        if (dbPointer != null ? !dbPointer.equals(that.dbPointer) : that.dbPointer != null) return false;
        if (javaScript != null ? !javaScript.equals(that.javaScript) : that.javaScript != null) return false;
        if (javaScriptWithScope != null ? !javaScriptWithScope.equals(that.javaScriptWithScope) : that.javaScriptWithScope != null)
            return false;
        if (minKey != null ? !minKey.equals(that.minKey) : that.minKey != null) return false;
        if (maxKey != null ? !maxKey.equals(that.maxKey) : that.maxKey != null) return false;
        if (regularExpression != null ? !regularExpression.equals(that.regularExpression) : that.regularExpression != null) return false;
        if (symbol != null ? !symbol.equals(that.symbol) : that.symbol != null) return false;
        if (timestamp != null ? !timestamp.equals(that.timestamp) : that.timestamp != null) return false;
        return undefined != null ? undefined.equals(that.undefined) : that.undefined == null;

    }

    @Override
    public int hashCode() {
        int result = _id != null ? _id.hashCode() : 0;
        result = 31 * result + (binary != null ? binary.hashCode() : 0);
        result = 31 * result + (dbPointer != null ? dbPointer.hashCode() : 0);
        result = 31 * result + (javaScript != null ? javaScript.hashCode() : 0);
        result = 31 * result + (javaScriptWithScope != null ? javaScriptWithScope.hashCode() : 0);
        result = 31 * result + (minKey != null ? minKey.hashCode() : 0);
        result = 31 * result + (maxKey != null ? maxKey.hashCode() : 0);
        result = 31 * result + (regularExpression != null ? regularExpression.hashCode() : 0);
        result = 31 * result + (symbol != null ? symbol.hashCode() : 0);
        result = 31 * result + (timestamp != null ? timestamp.hashCode() : 0);
        result = 31 * result + (undefined != null ? undefined.hashCode() : 0);
        return result;
    }
}
