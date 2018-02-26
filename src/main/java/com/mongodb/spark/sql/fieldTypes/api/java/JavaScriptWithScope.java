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

import org.bson.BsonDocument;

/**
 * A Java bean representing the Bson JavaScriptWithCode type
 *
 * @since 1.0
 */
public final class JavaScriptWithScope {
    private String code;
    private String scope;

    /**
     * Construct a new instance
     */
    public JavaScriptWithScope() {
    }

    /**
     * Construct a new instance
     *
     * @param code the javascript code
     * @param scope a bson document representing the scope
     */
    public JavaScriptWithScope(final String code, final BsonDocument scope) {
        this(code, Assertions.notNull("scope", scope.toJson()));
    }

    /**
     * Construct a new instance
     *
     * @param code the javascript code
     * @param scope the json representation of the scope
     */
    public JavaScriptWithScope(final String code, final String scope) {
        this.code = Assertions.notNull("code", code);
        this.scope = Assertions.notNull("scope", scope);
    }

    public String getCode() {
        return code;
    }

    public void setCode(final String code) {
        this.code = Assertions.notNull("code", code);
    }

    public String getScope() {
        return scope;
    }

    public void setScope(final String scope) {
        this.scope = Assertions.notNull("scope", scope);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        JavaScriptWithScope that = (JavaScriptWithScope) o;

        if (code != null ? !code.equals(that.code) : that.code != null) {
            return false;
        }
        return scope != null ? scope.equals(that.scope) : that.scope == null;

    }

    @Override
    public int hashCode() {
        int result = code != null ? code.hashCode() : 0;
        result = 31 * result + (scope != null ? scope.hashCode() : 0);
        return result;
    }
}
