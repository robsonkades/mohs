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
package io.mohs.rest.overview;

import java.time.Duration;
import java.util.Objects;

import org.jspecify.annotations.Nullable;
import org.springframework.boot.convert.DurationStyle;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import io.mohs.core.Mohs;
import io.mohs.rest.ApiPaths;
import io.mohs.rest.CursorPage;
import io.mohs.rest.error.PayloadValidationException;

/**
 * {@code GET /overview} — the dashboard's polling anchor:
 * live-work counts plus the terminal throughput of the recent window, all through the {@link Mohs}
 * facade (REST does not see the engine — the ArchitectureTest boundary).
 *
 * <p>{@code /stream} is the same snapshot pushed over SSE — see {@link OverviewStreamBroadcaster}.
 */
@RestController
@RequestMapping("${mohs.api.base-path:" + ApiPaths.V1 + "}/overview")
public class OverviewController {

    /** The {@code ?window=} default — 60s reads as "throughput per minute" with no arithmetic on the consumer's side. The applied window travels in the response ({@code throughput.window}). */
    static final Duration DEFAULT_THROUGHPUT_WINDOW = Duration.ofSeconds(60);

    /**
     * The {@code ?window=} clamp, in the same idiom as {@link CursorPage#clampSize} — the ceiling
     * protects the "cheap by construction" contract: the count's cost is proportional to the window,
     * and above an hour that is analytics over history, not a dashboard. Whoever asks for more gets
     * the ceiling and SEES the ceiling in the response.
     */
    static final Duration MIN_THROUGHPUT_WINDOW = Duration.ofSeconds(1);
    static final Duration MAX_THROUGHPUT_WINDOW = Duration.ofHours(1);

    private final Mohs mohs;
    private final OverviewStreamBroadcaster broadcaster;

    public OverviewController(Mohs mohs, OverviewStreamBroadcaster broadcaster) {
        this.mohs = Objects.requireNonNull(mohs, "mohs");
        this.broadcaster = Objects.requireNonNull(broadcaster, "broadcaster");
    }

    /**
     * {@code ?window=} as a {@code String} plus an explicit parse
     * ({@link DurationStyle#detectAndParse}, which accepts both {@code 15m} and {@code PT15M}),
     * never the host's binder: Mohs is an embedded library, and depending on the host application's
     * MVC {@code ConversionService} would make the accepted format vary per host. An unparseable
     * value becomes a 422 that teaches, not a 500.
     */
    @GetMapping
    public OverviewResponse overview(@RequestParam(required = false) @Nullable String window) {
        return OverviewResponse.from(mohs.overview(resolveWindow(window)));
    }

    private static Duration resolveWindow(@Nullable String window) {
        if (window == null || window.isBlank()) {
            return DEFAULT_THROUGHPUT_WINDOW;
        }
        Duration parsed = parseWindow(window);
        if (parsed.compareTo(MIN_THROUGHPUT_WINDOW) < 0) {
            return MIN_THROUGHPUT_WINDOW;
        }
        if (parsed.compareTo(MAX_THROUGHPUT_WINDOW) > 0) {
            return MAX_THROUGHPUT_WINDOW;
        }
        return parsed;
    }

    private static Duration parseWindow(String window) {
        try {
            return DurationStyle.detectAndParse(window);
        } catch (IllegalArgumentException invalidFormat) {
            throw new PayloadValidationException("window", "window must be a duration like 15m, 90s or PT15M, got '" + window + "'");
        }
    }

    @GetMapping("/stream")
    public SseEmitter stream() {
        return broadcaster.subscribe();
    }
}
