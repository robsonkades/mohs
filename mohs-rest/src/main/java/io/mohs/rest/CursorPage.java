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

import java.util.List;
import java.util.Objects;
import java.util.function.Function;

import org.jspecify.annotations.Nullable;

/**
 * A result page using keyset pagination (PoEAA/DDIA) — an absent {@code nextCursor} marks the last
 * page.
 *
 * <p>Used only on the genuinely unbounded listings ({@code GET /executions},
 * {@code GET /jobs/{jobKey}/executions}); listings of bounded cardinality (jobs, queues, rate
 * limits, runners, nodes) return a {@code List<T>} directly.
 *
 * <p>Page size: the optional {@code size} parameter on both endpoints above —
 * {@link #DEFAULT_PAGE_SIZE} when absent, with {@link #MAX_PAGE_SIZE} as a hard ceiling (asking for
 * more is not an error, it saturates at the ceiling). Decided at contract time because an unbounded
 * page over an unbounded table is a real denial-of-service surface.
 */
public record CursorPage<T>(List<T> items, @Nullable String nextCursor) {

    public static final int DEFAULT_PAGE_SIZE = 50;
    public static final int MAX_PAGE_SIZE = 200;

    public CursorPage {
        Objects.requireNonNull(items, "items");
        items = List.copyOf(items);
    }

    /**
     * Builds the page from a result fetched with {@code size + 1} items ({@code HistoryStore#findPage}'s
     * convention) — the extra item reveals whether there is a next page without an additional round
     * trip, and is dropped from the returned body.
     *
     * @param fetched at most {@code size + 1} items
     * @param size the page size requested by the caller
     * @param cursorOf extracts the opaque cursor from the last item of a full page
     */
    public static <T> CursorPage<T> of(List<T> fetched, int size, Function<T, String> cursorOf) {
        boolean hasMore = fetched.size() > size;
        List<T> page = hasMore ? fetched.subList(0, size) : fetched;
        return new CursorPage<>(page, hasMore ? cursorOf.apply(page.get(page.size() - 1)) : null);
    }

    /**
     * Normalises the {@code size} the client asked for — every request parameter is hostile input
     * until validated: it saturates at {@link #MAX_PAGE_SIZE} above and at {@code 1} below
     * ({@code 0} or a negative value would blow up as a 500 inside {@code ExecutionQuery}/{@link #of},
     * which is never the right validation mechanism).
     */
    public static int clampSize(@Nullable Integer requested) {
        return requested == null ? DEFAULT_PAGE_SIZE : Math.clamp(requested, 1, MAX_PAGE_SIZE);
    }
}
