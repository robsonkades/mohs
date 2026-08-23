package io.mohs.engine;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.Test;

import io.github.robsonkades.uuidv7.UUIDv7;

import io.mohs.core.execution.ExecutionId;

import static org.assertj.core.api.Assertions.assertThat;

class ShardsTest {

    /**
     * Literais fixados de propósito (contrato de estabilidade do Javadoc de
     * {@link Shards}): o shard é GRAVADO em {@code mohs_ready}/{@code
     * mohs_execution} — se a função mudar entre versões, um rolling upgrade
     * deixa linhas antigas em shards que ninguém mais deriva. Este teste
     * quebrando = a mudança exige migração de dados, não é refactor.
     */
    @Test
    void hashIsPinnedAcrossVersions() {
        assertThat(Shards.of(ExecutionId.of("018f7f2a-1111-7abc-8def-000000000001"))).isEqualTo(53);
        assertThat(Shards.of(ExecutionId.of("018f7f2a-1111-7abc-8def-000000000002"))).isEqualTo(60);
        assertThat(Shards.of(ExecutionId.of("0663b3aa-51a6-7f5f-b81a-524a70b2f0ee"))).isEqualTo(3);
        assertThat(Shards.of(ExecutionId.of("ffffffff-ffff-7fff-bfff-ffffffffffff"))).isEqualTo(24);
    }

    @Test
    void spreadsUuidV7IdsAcrossEveryShard() {
        Set<Integer> seen = new HashSet<>();
        for (int i = 0; i < 10_000; i++) {
            int shard = Shards.of(ExecutionId.of(UUIDv7.randomUUIDString()));
            assertThat(shard).isBetween(0, Shards.SHARD_COUNT - 1);
            seen.add(shard);
        }
        // com 10k amostras em 64 baldes, um balde vazio denunciaria viés
        // estrutural do hash sobre o formato UUIDv7 (prefixo de timestamp
        // quase constante), não flutuação estatística
        assertThat(seen).hasSize(Shards.SHARD_COUNT);
    }

    @Test
    void everyShardHasExactlyOneOwnerAcrossTheCluster() {
        List<String> nodes = List.of("node-c", "node-a", "node-b");
        Set<Integer> covered = new HashSet<>();
        int total = 0;
        for (String node : nodes) {
            List<Integer> owned = Shards.ownedBy(node, nodes);
            total += owned.size();
            covered.addAll(owned);
        }
        // cobertura total E total == 64: junto, prova partição (sem furo, sem sobreposição)
        assertThat(covered).hasSize(Shards.SHARD_COUNT);
        assertThat(total).isEqualTo(Shards.SHARD_COUNT);
    }

    /** A atribuição deriva da ORDEM dos ids, não da ordem da lista — todo node precisa chegar à mesma partição sem negociar. */
    @Test
    void assignmentIsIndependentOfListOrder() {
        assertThat(Shards.ownedBy("node-b", List.of("node-c", "node-a", "node-b")))
                .isEqualTo(Shards.ownedBy("node-b", List.of("node-a", "node-b", "node-c")));
    }

    @Test
    void singleNodeOwnsEveryShard() {
        assertThat(Shards.ownedBy("node-a", List.of("node-a"))).hasSize(Shards.SHARD_COUNT);
    }

    /** Degeneração segura: fora da lista (foto atrasada de mohs_nodes) ou lista vazia → possui TODOS — sobreposição é o comportamento pré-shard (SKIP LOCKED resolve), fila parada não é opção. */
    @Test
    void nodeMissingFromTheListOwnsEveryShard() {
        assertThat(Shards.ownedBy("node-x", List.of("node-a", "node-b"))).hasSize(Shards.SHARD_COUNT);
        assertThat(Shards.ownedBy("node-x", List.of())).hasSize(Shards.SHARD_COUNT);
    }
}
