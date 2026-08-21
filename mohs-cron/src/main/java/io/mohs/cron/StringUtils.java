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
 * This file extracts the three string-splitting methods CronExpression/CronField call out of
 * Spring Framework's org.springframework.util.StringUtils
 * (https://github.com/spring-projects/spring-framework), under the same license, so this module
 * doesn't need Spring itself as a dependency just for cron-field tokenizing. Logic is otherwise
 * unchanged from upstream (same StringTokenizer-based splitting, same literal-substring replace).
 */
package io.mohs.cron;

import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.jspecify.annotations.Nullable;

/**
 * The subset of {@code org.springframework.util.StringUtils} this
 * package's copied cron code calls. {@code str} is {@code @Nullable} on
 * the two methods below (CRON-1) — each already handles it explicitly
 * (returns an empty array), matching upstream Spring's own annotation
 * that the initial JSpecify port dropped.
 */
final class StringUtils {

    private StringUtils() {
    }

    /** Splits {@code str} on any character in {@code delimiters}, trimming tokens and dropping empty ones. */
    static String[] tokenizeToStringArray(@Nullable String str, String delimiters) {
        if (str == null) {
            return new String[0];
        }
        StringTokenizer tokenizer = new StringTokenizer(str, delimiters);
        List<String> tokens = new ArrayList<>();
        while (tokenizer.hasMoreTokens()) {
            String token = tokenizer.nextToken().trim();
            if (!token.isEmpty()) {
                tokens.add(token);
            }
        }
        return tokens.toArray(new String[0]);
    }

    /** Splits {@code str} on the literal (possibly multi-character) {@code delimiter} — no trimming. */
    static String[] delimitedListToStringArray(@Nullable String str, @Nullable String delimiter) {
        if (str == null) {
            return new String[0];
        }
        if (delimiter == null) {
            return new String[] {str};
        }
        List<String> result = new ArrayList<>();
        int pos = 0;
        int delimiterPos;
        while ((delimiterPos = str.indexOf(delimiter, pos)) != -1) {
            result.add(str.substring(pos, delimiterPos));
            pos = delimiterPos + delimiter.length();
        }
        if (!str.isEmpty() && pos <= str.length()) {
            result.add(str.substring(pos));
        }
        return result.toArray(new String[0]);
    }

    /** Replaces every literal occurrence of {@code oldPattern} in {@code inString} with {@code newPattern}. */
    static String replace(String inString, String oldPattern, String newPattern) {
        if (inString == null || inString.isEmpty() || oldPattern == null || oldPattern.isEmpty() || newPattern == null) {
            return inString;
        }
        int index = inString.indexOf(oldPattern);
        if (index == -1) {
            return inString;
        }
        StringBuilder result = new StringBuilder(inString.length());
        int pos = 0;
        int patternLength = oldPattern.length();
        while (index >= 0) {
            result.append(inString, pos, index).append(newPattern);
            pos = index + patternLength;
            index = inString.indexOf(oldPattern, pos);
        }
        result.append(inString, pos, inString.length());
        return result.toString();
    }
}
