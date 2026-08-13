/**
 * API pública do Mohs. Este pacote raiz carrega a fachada
 * ({@link io.mohs.Mohs}, {@link io.mohs.MohsLifecycle},
 * {@link io.mohs.ScheduleCommand}, {@link io.mohs.Batch},
 * {@link io.mohs.BatchBuilder}) e a identidade compartilhada
 * ({@link io.mohs.JobKey}, {@link io.mohs.ExecutionId},
 * {@link io.mohs.JobRef}) — os tipos que um consumidor típico importa
 * primeiro. O restante do vocabulário público vive em subpacotes coesos:
 * {@code io.mohs.schedule}, {@code io.mohs.definition},
 * {@code io.mohs.execution}, {@code io.mohs.event},
 * {@code io.mohs.resource} (ver {@code docs/adr/0013-public-api-subpackaging.md}).
 *
 * <p>Tudo aqui e nos subpacotes acima é contrato — records, interfaces
 * seladas e interfaces simples, sem fiação de motor. A implementação real
 * vive em {@code io.mohs.engine} e {@code io.mohs.jdbc}, dos quais nenhum
 * tipo público pode depender (regra ArchUnit).
 */
package io.mohs;
