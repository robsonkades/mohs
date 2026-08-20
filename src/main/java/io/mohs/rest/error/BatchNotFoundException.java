package io.mohs.rest.error;

import java.io.Serial;
import java.util.Objects;

/** Lote inexistente numa rota que espera um {@code batchId} válido. */
public final class BatchNotFoundException extends RuntimeException {

    @Serial
    private static final long serialVersionUID = 1L;

    private final String batchId;

    public BatchNotFoundException(String batchId) {
        super("Batch not found: " + batchId);
        this.batchId = Objects.requireNonNull(batchId, "batchId");
    }

    public String batchId() {
        return batchId;
    }
}
