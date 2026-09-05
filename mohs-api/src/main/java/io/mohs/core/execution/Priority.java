/*
 * Copyright 2026 The Mohs Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.mohs.core.execution;

/**
 * The priority of a scheduled instance, in five levels.
 *
 * <p>{@link #value()} is the weight used to order candidates at claim time — a lower value is
 * claimed first (Effective Java Item 34: associated data becomes an instance field, never
 * {@code ordinal()}).
 *
 * <p>There is no aging in this version, so {@link #BACKGROUND} can starve under sustained load from
 * higher levels — a documented risk.
 */
public enum Priority {
    /** Claims before everything else. */
    CRITICAL(0),
    /** Claims before {@link #NORMAL}. */
    HIGH(10),
    /** The default. */
    NORMAL(20),
    /** Claims after {@link #NORMAL}. */
    LOW(30),
    /** Claims last. */
    BACKGROUND(40);

    /** {@code values()} clones the array on every call — cached because {@link #fromValue} runs per row mapped at the claim's JDBC boundary. */
    private static final Priority[] VALUES = values();

    private final int value;

    Priority(int value) {
        this.value = value;
    }

    /**
     * Returns the integer stored in the queue priority column.
     *
     * @return the database representation of this priority
     */
    public int value() {
        return value;
    }

    /**
     * The inverse of {@link #value()} — used at the JDBC boundary, where only the {@code priority} column (an int) is written. Zero allocation: it runs per row mapped by the claim.
     *
     * @param value the serialized identity value
     * @return the priority represented by the stored integer
     */
    public static Priority fromValue(int value) {
        for (Priority priority : VALUES) {
            if (priority.value == value) {
                return priority;
            }
        }
        throw new IllegalArgumentException("no Priority with value " + value);
    }
}
