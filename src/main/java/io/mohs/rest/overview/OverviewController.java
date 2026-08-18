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
 * {@code GET /overview} — âncora de polling do dashboard (ver
 * {@code docs/REST-API-DESIGN.md}): contagens do trabalho vivo + vazão
 * terminal da janela recente, tudo pela fachada {@link Mohs} (a REST não
 * enxerga o motor — fronteira do ArchitectureTest). {@code /stream} é o
 * mesmo retrato empurrado por SSE — ver
 * {@link OverviewStreamBroadcaster}.
 */
@RestController
@RequestMapping("${mohs.api.base-path:" + ApiPaths.V1 + "}/overview")
public class OverviewController {

    /** Default do {@code ?window=} — 60s lê como "vazão por minuto" sem aritmética do lado do consumidor. A janela aplicada viaja na resposta ({@code throughput.window}). */
    static final Duration DEFAULT_THROUGHPUT_WINDOW = Duration.ofSeconds(60);

    /**
     * Clamp do {@code ?window=}, mesmo idioma de {@link CursorPage#clampSize}
     * — o teto protege o contrato "barato por construção": o custo da
     * contagem é proporcional à janela, e acima de 1h isso é analytics
     * sobre histórico, não dashboard. Quem pediu mais recebe o teto e VÊ o
     * teto na resposta.
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
     * {@code ?window=} como {@code String} + parse explícito
     * ({@link DurationStyle#detectAndParse}: aceita {@code 15m} e
     * {@code PT15M}), nunca binder do host: Mohs é biblioteca embarcada —
     * depender do {@code ConversionService} MVC da aplicação hospedeira
     * faria o formato aceito variar por host. Valor imparseável → 422 que
     * ensina, não 500.
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
