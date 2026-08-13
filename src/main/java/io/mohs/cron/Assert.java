/*
 * Copyright 2002-present the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * This file extracts the four assertion methods CronExpression/CronField/BitsCronField/
 * CompositeCronField/QuartzCronField call out of Spring Framework's org.springframework.util.Assert
 * (https://github.com/spring-projects/spring-framework), under the same license, so this module
 * doesn't need Spring itself as a dependency just for a handful of null/empty checks. Everything
 * else about those five classes is otherwise unchanged from upstream.
 */
package io.mohs.cron;

/** The subset of {@code org.springframework.util.Assert} this package's copied cron code calls. */
final class Assert {

    private Assert() {
    }

    static void isTrue(boolean expression, String message) {
        if (!expression) {
            throw new IllegalArgumentException(message);
        }
    }

    static void notNull(Object object, String message) {
        if (object == null) {
            throw new IllegalArgumentException(message);
        }
    }

    static void hasLength(String text, String message) {
        if (text == null || text.isEmpty()) {
            throw new IllegalArgumentException(message);
        }
    }

    static void notEmpty(Object[] array, String message) {
        if (array == null || array.length == 0) {
            throw new IllegalArgumentException(message);
        }
    }
}
