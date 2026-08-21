/**
 * Identidade de um job: {@link io.mohs.core.job.JobKey} (chave estável) e
 * {@link io.mohs.core.job.JobRef} (referência tipada). Extraído à parte de
 * {@code io.mohs.core.definition} porque a identidade é compartilhada por
 * pacotes pares que não deveriam depender uns dos outros —
 * {@code io.mohs.core.definition}, {@code io.mohs.core.execution},
 * {@code io.mohs.core.event} e a fachada em {@code io.mohs.core} — todos
 * dependem daqui, não entre si.
 */
@NullMarked
package io.mohs.core.job;

import org.jspecify.annotations.NullMarked;
