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
package io.mohs.rest;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CursorPageTest {

    @Test
    void copiesItemsDefensively() {
        List<String> mutable = new ArrayList<>(List.of("a", "b"));
        CursorPage<String> page = new CursorPage<>(mutable, "next-cursor");

        mutable.add("c");

        assertThat(page.items()).containsExactly("a", "b");
    }

    @Test
    void nextCursorAbsentMarksTheLastPage() {
        CursorPage<String> page = new CursorPage<>(List.of("a"), null);

        assertThat(page.nextCursor()).isNull();
    }

    @Test
    void rejectsNullItems() {
        assertThatThrownBy(() -> new CursorPage<String>(null, null))
                .isInstanceOf(NullPointerException.class);
    }

    /** Saturation in both directions — every request parameter is hostile until validated; the boundary normalises, never blows up. */
    @Test
    void clampSizeSaturatesAtBothEnds() {
        assertThat(CursorPage.clampSize(null)).isEqualTo(CursorPage.DEFAULT_PAGE_SIZE);
        assertThat(CursorPage.clampSize(0)).isEqualTo(1);
        assertThat(CursorPage.clampSize(-1)).isEqualTo(1);
        assertThat(CursorPage.clampSize(25)).isEqualTo(25);
        assertThat(CursorPage.clampSize(CursorPage.MAX_PAGE_SIZE + 1)).isEqualTo(CursorPage.MAX_PAGE_SIZE);
    }
}
