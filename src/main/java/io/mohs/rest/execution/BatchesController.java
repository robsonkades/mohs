package io.mohs.rest.execution;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/** {@code GET /batches/{id}} — contadores agregados e estado do lote. */
@RestController
@RequestMapping("/api/mohs/v1/batches")
public class BatchesController {

    @GetMapping("/{id}")
    public BatchResponse get(@PathVariable String id) {
        throw new UnsupportedOperationException("M3: ainda não implementado");
    }
}
