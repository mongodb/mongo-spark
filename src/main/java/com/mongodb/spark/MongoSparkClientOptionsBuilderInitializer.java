/*
 * Copyright (c) 2008-2015 MongoDB, Inc.
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

package com.mongodb.spark;

import com.mongodb.MongoClientOptions.Builder;

import java.util.function.Supplier;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * An implementation of a MongoClientOptionsBuilderInitializer. This implementation allows
 * additional client options not supported by a MongoClientURI to be configured and passed
 * to worker nodes.
 */
public class MongoSparkClientOptionsBuilderInitializer implements MongoClientOptionsBuilderInitializer {
    private Supplier<Builder> builderSupplier;

    /**
     * Constructs an instance. Users should provide a function that supplies a Builder, e.g.
     *
     * <pre>
     * {@code
     * MongoSparkClientOptionsBuilderInitializer initializer =
     *     new MongoSparkClientOptionsBuilderInitializer(() -> new Builder().maxConnectionLifeTime(100));
     * }
     * </pre>
     *
     * @see com.mongodb.spark.SerializableSupplier
     *
     * @param builderSupplier a non-null serializable supplier function
     */
    public MongoSparkClientOptionsBuilderInitializer(final SerializableSupplier<Builder> builderSupplier) {
        notNull("builderSupplier", builderSupplier);
        this.builderSupplier = builderSupplier;
    }

    @Override
    public Builder initialize() {
        return this.builderSupplier.get();
    }
}
