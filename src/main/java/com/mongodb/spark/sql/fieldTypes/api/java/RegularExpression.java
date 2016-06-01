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

import java.util.regex.Pattern;

import static com.mongodb.spark.sql.fieldTypes.api.java.Assertions.notNull;

/**
 * A Java bean representing the Bson RegularExpression type
 *
 * @since 1.0
 */
public final class RegularExpression {
    private String regex;
    private String options;

    /**
     * Construct a new instance
     */
    public RegularExpression() {
    }

    /**
     * Construct a new instance
     *
     * @param pattern the regular expression pattern
     */
    public RegularExpression(final Pattern pattern) {
        notNull("pattern", pattern);
        com.mongodb.spark.sql.fieldTypes.RegularExpression scalaRegEx = com.mongodb.spark.sql.fieldTypes.RegularExpression.apply(pattern);
        this.regex = scalaRegEx.regex();
        this.options = scalaRegEx.options();
    }

    /**
     * Construct a new instance
     *
     * @param regex the regular expression
     * @param options the options
     */
    public RegularExpression(final String regex, final String options) {
        this.regex = notNull("regex", regex);
        this.options = notNull("options", options);
    }

    public String getRegex() {
        return regex;
    }

    public void setRegex(final String regex) {
        this.regex = notNull("regex", regex);
    }

    public String getOptions() {
        return options;
    }

    public void setOptions(final String options) {
        this.options = notNull("options", options);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        RegularExpression that = (RegularExpression) o;

        if (regex != null ? !regex.equals(that.regex) : that.regex != null) {
            return false;
        }
        return options != null ? options.equals(that.options) : that.options == null;
    }

    @Override
    public int hashCode() {
        int result = regex != null ? regex.hashCode() : 0;
        result = 31 * result + (options != null ? options.hashCode() : 0);
        return result;
    }
}
