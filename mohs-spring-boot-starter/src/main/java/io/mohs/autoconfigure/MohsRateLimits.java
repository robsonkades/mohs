package io.mohs.autoconfigure;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mohs.core.RateLimitSnapshot;
import io.mohs.core.resource.RateLimit;
import io.mohs.engine.RateLimitStore;

/**
 * Monta os {@link RateLimit} declarados (beans + propriedades) e os
 * registra no store no boot — o equivalente de {@link MohsRunners} para o
 * eixo de vazão, com uma diferença que muda tudo: runner é estado local do
 * nó, rate limit é estado COMPARTILHADO, então registrar aqui é escrever
 * no banco e a divergência com o que já está lá segue a política
 * {@code mohs.registration.on-conflict} (ADR-0006), como a definição de
 * job.
 *
 * <p>O balde de tokens nunca é tocado por este caminho — quem preserva o
 * saldo é {@link RateLimitStore#upsert} (ADR-0042): boot manda na spec,
 * jamais no estado corrente.
 */
final class MohsRateLimits {

    private static final Logger log = LoggerFactory.getLogger(MohsRateLimits.class);

    private MohsRateLimits() {
    }

    /**
     * Bean define a estrutura, propriedade ajusta os números (mesmo
     * contrato de {@code docs/API-DESIGN.md} para recursos nomeados): um
     * nome declarado nos dois lugares fica com o valor da propriedade, que
     * é o que se muda sem recompilar.
     */
    static List<RateLimit> assemble(MohsProperties properties, List<RateLimit> beanRateLimits) {
        Objects.requireNonNull(properties, "properties");
        Objects.requireNonNull(beanRateLimits, "beanRateLimits");

        Map<String, RateLimit> byName = new LinkedHashMap<>();
        for (RateLimit beanRateLimit : beanRateLimits) {
            RateLimit previous = byName.put(beanRateLimit.name(), beanRateLimit);
            if (previous != null) {
                throw new IllegalArgumentException("duplicate rate limit name '" + beanRateLimit.name()
                        + "' — two @Bean RateLimit declare the same name");
            }
        }
        properties.rateLimits().forEach((name, spec) -> byName.put(name, toRateLimit(name, spec)));
        return List.copyOf(byName.values());
    }

    /**
     * Os dois campos são obrigatórios e a falta de qualquer um derruba o
     * boot com o nome da propriedade que falta — validação de boot no
     * lugar de um limite que só se revelaria errado sob carga (§5.13).
     */
    private static RateLimit toRateLimit(String name, MohsProperties.RateLimitSpec spec) {
        if (spec.max() == null) {
            throw new IllegalArgumentException("rate limit '" + name + "' is missing mohs.rate-limits." + name + ".max");
        }
        if (spec.window() == null) {
            throw new IllegalArgumentException("rate limit '" + name + "' is missing mohs.rate-limits." + name + ".window");
        }
        return new RateLimit(name, spec.max(), spec.window());
    }

    /** Divergência entre o declarado e o armazenado segue {@code on-conflict}, exatamente como a definição de job em {@link MohsJobScanner}. */
    static void register(RateLimitStore store, MohsProperties.Registration.OnConflict onConflict, List<RateLimit> declared) {
        for (RateLimit incoming : declared) {
            Optional<RateLimit> existing = store.find(incoming.name()).map(RateLimitSnapshot::rateLimit);
            if (existing.isEmpty() || existing.get().equals(incoming)) {
                store.upsert(incoming);
                continue;
            }
            switch (onConflict) {
                case OVERRIDE -> {
                    log.info("rate limit '{}' changed, code wins (mohs.registration.on-conflict=override): {} -> {}",
                            incoming.name(), describe(existing.get()), describe(incoming));
                    store.upsert(incoming);
                }
                case PRESERVE -> log.warn(
                        "rate limit '{}' changed but store wins (mohs.registration.on-conflict=preserve), code version ignored: {} kept over {}",
                        incoming.name(), describe(existing.get()), describe(incoming));
                case FAIL -> throw new IllegalStateException("rate limit '" + incoming.name()
                        + "' diverged from the stored one (mohs.registration.on-conflict=fail): "
                        + describe(existing.get()) + " stored, " + describe(incoming) + " declared");
            }
        }
    }

    private static String describe(RateLimit rateLimit) {
        return rateLimit.max() + "/" + rateLimit.window();
    }
}
