package io.mohs.engine;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.IntStream;

import io.mohs.core.execution.ExecutionId;

/**
 * O shard de uma execução e a atribuição de shards a nós (§8.3 do
 * redesign, ADR-F). {@code shard = hash(execution_id) % 64} — FUNÇÃO do
 * id, nunca estado transportado: enqueue, retry, requeue e reaper
 * re-derivam o mesmo valor de onde estiverem, sem coluna em
 * {@code mohs_lease} e sem migração (decisão 1 do PLAN.md da Phase 6).
 *
 * <p>{@code SHARD_COUNT = 64} é fixo, não configurável: 64 divide
 * limpo qualquer contagem plausível de nós e 64 shards num nó só não
 * custam nada (§8.3). O hash é FNV-1a sobre os bytes UTF-8 do id —
 * NUNCA {@code String.hashCode}: o shard gravado por uma JVM precisa ser
 * re-derivável por qualquer outra, para sempre; a estabilidade é
 * contrato, pinada por valores literais em {@code ShardsTest}. Mudar a
 * função re-embaralharia o backlog persistido (entradas ficariam em
 * shards que ninguém re-deriva igual) — é o tipo de mudança que exige
 * migração, não um refactor.
 *
 * <p>A atribuição é DERIVADA, não negociada (§8.3): cada nó ordena os
 * ids vivos, acha o próprio índice {@code i} de {@code n} e possui
 * {@code { s : s % n == i }}. Sem líder, sem lock; dois nós podem
 * divergir por um heartbeat durante mudança de membership — sobreposição
 * degrada exatamente pro comportamento pré-shard ({@code SKIP LOCKED}
 * resolve) e se cura sozinha em um heartbeat.
 */
public final class Shards {

    /** Fixo por decisão (§8.3) — não é knob. */
    public static final int SHARD_COUNT = 64;

    private static final int FNV_OFFSET_BASIS = 0x811c9dc5;
    private static final int FNV_PRIME = 0x01000193;

    private Shards() {
    }

    /** O shard desta execução — determinístico entre JVMs e versões (contrato pinado em {@code ShardsTest}). */
    public static int of(ExecutionId executionId) {
        Objects.requireNonNull(executionId, "executionId");
        int hash = FNV_OFFSET_BASIS;
        for (byte b : executionId.value().getBytes(StandardCharsets.UTF_8)) {
            hash ^= (b & 0xff);
            hash *= FNV_PRIME;
        }
        return Math.floorMod(hash, SHARD_COUNT);
    }

    /**
     * Os shards que {@code nodeId} possui dado o conjunto de nós
     * elegíveis — a lista é ordenada AQUI (cópia), então o chamador passa
     * os ids em qualquer ordem e dois nós com o mesmo conjunto derivam a
     * mesma partição. Nó fora do conjunto (ex.: acabou de entrar e ainda
     * não se vê na leitura) possui TODOS os shards — o degenerado seguro:
     * sobreposição temporária é o comportamento pré-shard, enquanto
     * "possuir nada" deixaria fila parada.
     */
    public static List<Integer> ownedBy(String nodeId, List<String> eligibleNodeIds) {
        Objects.requireNonNull(nodeId, "nodeId");
        List<String> sorted = Objects.requireNonNull(eligibleNodeIds, "eligibleNodeIds").stream().sorted().toList();
        int index = sorted.indexOf(nodeId);
        if (index < 0) {
            return everyShard();
        }
        List<Integer> owned = new ArrayList<>();
        for (int shard = index; shard < SHARD_COUNT; shard += sorted.size()) {
            owned.add(shard);
        }
        return owned;
    }

    /** A mesma posse como bitmask — {@code SHARD_COUNT == 64} cabe exato num {@code long}, e um long {@code volatile} é o que o filtro do LISTEN lê de fora da thread do tick (JLS 17.7). */
    public static long maskOf(List<Integer> ownedShards) {
        long mask = 0L;
        for (int shard : ownedShards) {
            mask |= 1L << shard;
        }
        return mask;
    }

    private static List<Integer> everyShard() {
        return IntStream.range(0, SHARD_COUNT).boxed().toList();
    }
}
