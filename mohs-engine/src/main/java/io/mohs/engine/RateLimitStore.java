package io.mohs.engine;

import java.time.Instant;
import java.util.Optional;
import java.util.stream.Stream;

import io.mohs.core.RateLimitSnapshot;
import io.mohs.core.resource.RateLimit;

/**
 * Persistência de {@link RateLimit} — Repository (PoEAA), porta que
 * {@code io.mohs.store.jdbc} implementa. Guarda também o estado operacional do
 * enforcement: o balde de tokens da ADR-0042 (saldo + instante até o qual
 * o tempo já virou token), lido por {@link #available} e cobrado por
 * {@link #charge} nas duas fases do claim.
 */
public interface RateLimitStore {

    /**
     * Grava a spec — e só a spec. O balde é estado operacional e sobrevive
     * ao upsert (tokens clampados ao novo {@code max}): resetar aqui faria
     * cada nó que sobe num rolling deploy devolver um balde cheio,
     * transformando deploy em burst. Mesmo raciocínio de {@code paused} em
     * {@link JobStore#upsert} (ADR-0006/0037): configuração de boot manda
     * na spec, nunca no estado corrente.
     */
    RateLimit upsert(RateLimit rateLimit);

    /**
     * FASE 1 do consumo em duas fases (ADR-0042 rev. 2026-08-18): quantos
     * tokens o balde concederia agora. Leitura pura — sem lock, sem escrita,
     * e por isso sem custo de serialização: é o que permite decidir a
     * admissão do lote sem segurar a linha durante o claim inteiro.
     *
     * <p>Limite inexistente devolve 0 (fail-safe): job que aponta um nome
     * que não existe para de rodar em vez de rodar sem o limite que alguém
     * pediu — mesma postura de {@link ExecutionWindowRegistry#excludes}
     * para janela desconhecida.
     *
     * @param now instante do {@code Clock} injetado do chamador; o refill é calculado contra ele, nunca contra o relógio do banco
     */
    int available(String name, Instant now);

    /**
     * FASE 2: cobra exatamente {@code permits} do balde, tudo ou nada.
     * Chamada no FIM da transação de claim, depois do CAS — cobra o que foi
     * REIVINDICADO, não o que foi admitido, então token não queima em
     * candidato que perdeu o mutex do job. O lock de linha nasce aqui e
     * morre no commit: a janela de serialização vira a cauda da transação
     * em vez da transação inteira (medido: 2,3× a 4 clientes, 3,5× a 8).
     *
     * <p>A atomicidade vem de {@code UPDATE} guardado, não de lock
     * especializado — mesma disciplina da ADR-0018. Exige isolamento
     * READ COMMITTED: a implementação relê a linha entre tentativas do CAS, e
     * sob REPEATABLE READ a releitura devolveria o mesmo snapshot — as
     * tentativas falhariam idênticas, o retry viraria no-op caro.
     * {@code JdbcWorkQueue} garante isso explicitamente (DBTUNE-4); outro
     * chamador que herde uma transação {@code @Transactional} do host precisa
     * garantir o mesmo. {@code false} significa
     * que outro nó alterou o balde entre as duas fases e não há saldo:
     * o chamador DEVE desfazer a rodada, porque as execuções já foram
     * reivindicadas e entregá-las sem token seria sobre-entrega, a única
     * violação inaceitável do contrato.
     */
    boolean charge(String name, int permits, Instant now);

    /** A spec mais o saldo corrente do balde, refill aplicado em memória — leitura pura, não escreve nem consome. */
    Optional<RateLimitSnapshot> find(String name);

    /**
     * Stream sobre um cursor aberto — quem chama é dono do ciclo de vida
     * (try-with-resources). DBTUNE-7: no Postgres, isso só é verdade dentro
     * de uma transação (autocommit desligado) — fora dela, o driver
     * materializa o resultado inteiro antes de devolver o primeiro item.
     */
    Stream<RateLimitSnapshot> findAll();
}
