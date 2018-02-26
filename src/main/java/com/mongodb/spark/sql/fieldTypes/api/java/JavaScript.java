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

/**
 * A Java bean representing the Bson JavaScript type
 *
 * @since 1.0
 */
public final class JavaScript {
    private String code;

    /**
     * Construct a new instance
     */
    public JavaScript() {
    }

    /**
     * Construct a new instance
     *
     * @param code the javascript code
     */
    public JavaScript(final String code) {
        this.code = Assertions.notNull("code", code);
    }

    public String getCode() {
        return code;
    }

    public void setCode(final String code) {
        this.code = Assertions.notNull("code", code);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        JavaScript that = (JavaScript) o;

        return code != null ? code.equals(that.code) : that.code == null;
    }

    @Override
    public int hashCode() {
        return code != null ? code.hashCode() : 0;
    }
}
