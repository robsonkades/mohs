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
package io.mohs.store.jdbc;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;

/**
 * The {@code V*.sql} deltas of one dialect in the order an operator applies them on an upgrade — by
 * version number, not by name: a lexical sort would put {@code V10} before {@code V2}.
 */
final class MigrationChain {

    private MigrationChain() {
    }

    static List<Resource> deltasOf(String dialect) throws IOException {
        Resource[] deltas = new PathMatchingResourcePatternResolver()
                .getResources("classpath:io/mohs/store/jdbc/migration/" + dialect + "/V*.sql");
        Arrays.sort(deltas, Comparator.comparingInt(delta -> versionOf(delta.getFilename())));
        return List.of(deltas);
    }

    private static int versionOf(String filename) {
        return Integer.parseInt(filename.substring(1, filename.indexOf("__")));
    }
}
