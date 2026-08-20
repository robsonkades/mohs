package io.mohs.rest.batch;

import java.util.Objects;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import io.mohs.core.Mohs;
import io.mohs.rest.ApiPaths;
import io.mohs.rest.error.BatchNotFoundException;

/** {@code GET /batches/{id}} — contadores agregados e estado do lote. */
@RestController
@RequestMapping("${mohs.api.base-path:" + ApiPaths.V1 + "}/batches")
public class BatchesController {

    private final Mohs mohs;

    public BatchesController(Mohs mohs) {
        this.mohs = Objects.requireNonNull(mohs, "mohs");
    }

    /**
     * Seek na PK, custo plano no tamanho do lote — o contador mantido é o que
     * paga esta rota barata (ADR-0043). {@code pending} e {@code state} são
     * derivados dos três contadores por {@link BatchResponse#of}, e não
     * guardados: uma coluna a mais poderia divergir das outras, e não há
     * pergunta que ela responda mais rápido.
     */
    @GetMapping("/{id}")
    public BatchResponse get(@PathVariable String id) {
        return mohs.findBatch(id)
                .map(batch -> BatchResponse.of(batch.batchId(), batch.total(), batch.succeeded(), batch.failed()))
                .orElseThrow(() -> new BatchNotFoundException(id));
    }
}
