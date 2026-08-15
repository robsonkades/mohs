package io.mohs.rest.batch;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import io.mohs.rest.ApiPaths;

/** {@code GET /batches/{id}} — contadores agregados e estado do lote. */
@RestController
@RequestMapping("${mohs.api.base-path:" + ApiPaths.V1 + "}/batches")
public class BatchesController {

    @GetMapping("/{id}")
    public BatchResponse get(@PathVariable String id) {
        throw new UnsupportedOperationException("M3: ainda não implementado");
    }
}
