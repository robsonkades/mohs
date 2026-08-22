package io.mohs.autoconfigure;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import io.mohs.core.RateLimitSnapshot;
import io.mohs.core.resource.RateLimit;
import io.mohs.engine.RateLimitStore;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Montagem e registro dos limites declarados (ADR-0042) — o fio de ponta a ponta vive em {@code MohsAutoConfigurationTest}. */
class MohsRateLimitsTest {

    private static final RateLimit SMTP = new RateLimit("smtp", 100, Duration.ofMinutes(1));

    private static MohsProperties props(Map<String, MohsProperties.RateLimitSpec> rateLimits) {
        return new MohsProperties(
                true,
                new MohsProperties.Jdbc(null),
                new MohsProperties.Engine(Duration.ofSeconds(5), 50, 1, Duration.ofSeconds(30), null, Duration.ofSeconds(60), 64, 16, false),
                new MohsProperties.Lifecycle(MohsProperties.Lifecycle.StartMode.AUTO,
                        new MohsProperties.Lifecycle.Shutdown(Duration.ofSeconds(30))),
                new MohsProperties.Time(MohsProperties.Time.Mode.APPLICATION, Duration.ofSeconds(1), Duration.ofSeconds(30)),
                new MohsProperties.Registration(MohsProperties.Registration.OnConflict.OVERRIDE),
                new MohsProperties.Api(false, "/api/mohs/v1"),
                Map.of(),
                rateLimits);
    }

    @Test
    void assembleCollectsBeansAndProperties() {
        List<RateLimit> assembled = MohsRateLimits.assemble(
                props(Map.of("partner-api", new MohsProperties.RateLimitSpec(50, Duration.ofSeconds(10)))),
                List.of(SMTP));

        assertThat(assembled).containsExactlyInAnyOrder(SMTP, new RateLimit("partner-api", 50, Duration.ofSeconds(10)));
    }

    /** Bean define a estrutura, propriedade ajusta os números — é o que se muda sem recompilar. */
    @Test
    void thePropertyWinsOverABeanWithTheSameName() {
        List<RateLimit> assembled = MohsRateLimits.assemble(
                props(Map.of("smtp", new MohsProperties.RateLimitSpec(20, Duration.ofSeconds(5)))),
                List.of(SMTP));

        assertThat(assembled).containsExactly(new RateLimit("smtp", 20, Duration.ofSeconds(5)));
    }

    @Test
    void aRateLimitPropertyMissingMaxFailsTheBootNamingTheProperty() {
        assertThatThrownBy(() -> MohsRateLimits.assemble(
                props(Map.of("smtp", new MohsProperties.RateLimitSpec(null, Duration.ofMinutes(1)))), List.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("mohs.rate-limits.smtp.max");
    }

    @Test
    void aRateLimitPropertyMissingWindowFailsTheBootNamingTheProperty() {
        assertThatThrownBy(() -> MohsRateLimits.assemble(
                props(Map.of("smtp", new MohsProperties.RateLimitSpec(100, null))), List.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("mohs.rate-limits.smtp.window");
    }

    @Test
    void twoBeansWithTheSameNameFailTheBoot() {
        assertThatThrownBy(() -> MohsRateLimits.assemble(props(Map.of()),
                List.of(SMTP, new RateLimit("smtp", 5, Duration.ofSeconds(1)))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("duplicate rate limit name 'smtp'");
    }

    @Test
    void registerUpsertsALimitThatDoesNotExistYet() {
        RateLimitStore store = mock(RateLimitStore.class);
        when(store.find("smtp")).thenReturn(Optional.empty());

        MohsRateLimits.register(store, MohsProperties.Registration.OnConflict.OVERRIDE, List.of(SMTP));

        verify(store).upsert(SMTP);
    }

    /** Sob {@code preserve}, o PATCH de emergência sobrevive ao deploy — o código declarado é ignorado com WARN. */
    @Test
    void registerKeepsTheStoredValueUnderPreserve() {
        RateLimitStore store = mock(RateLimitStore.class);
        when(store.find("smtp")).thenReturn(Optional.of(
                new RateLimitSnapshot(new RateLimit("smtp", 20, Duration.ofMinutes(1)), 20)));

        MohsRateLimits.register(store, MohsProperties.Registration.OnConflict.PRESERVE, List.of(SMTP));

        verify(store, never()).upsert(SMTP);
    }

    @Test
    void registerFailsTheBootOnDivergenceUnderFail() {
        RateLimitStore store = mock(RateLimitStore.class);
        when(store.find("smtp")).thenReturn(Optional.of(
                new RateLimitSnapshot(new RateLimit("smtp", 20, Duration.ofMinutes(1)), 20)));

        assertThatThrownBy(() -> MohsRateLimits.register(store, MohsProperties.Registration.OnConflict.FAIL, List.of(SMTP)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("diverged");
    }
}
