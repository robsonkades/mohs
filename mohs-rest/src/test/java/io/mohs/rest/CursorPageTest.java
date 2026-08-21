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

    /** Saturação nos dois sentidos — todo parâmetro de request é hostil até validado; a borda normaliza, nunca estoura. */
    @Test
    void clampSizeSaturatesAtBothEnds() {
        assertThat(CursorPage.clampSize(null)).isEqualTo(CursorPage.DEFAULT_PAGE_SIZE);
        assertThat(CursorPage.clampSize(0)).isEqualTo(1);
        assertThat(CursorPage.clampSize(-1)).isEqualTo(1);
        assertThat(CursorPage.clampSize(25)).isEqualTo(25);
        assertThat(CursorPage.clampSize(CursorPage.MAX_PAGE_SIZE + 1)).isEqualTo(CursorPage.MAX_PAGE_SIZE);
    }
}
