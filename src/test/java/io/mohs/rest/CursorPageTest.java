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
}
