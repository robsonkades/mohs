/**
 * API pública do Mohs — "Contratos do core" no vocabulário de
 * {@code docs/MOHS-DOCUMENTO-MESTRE.md} (M1). Este pacote carrega a
 * fachada ({@link io.mohs.core.Mohs}, {@link io.mohs.core.MohsLifecycle},
 * {@link io.mohs.core.EngineState}, {@link io.mohs.core.ScheduleCommand})
 * e o recibo de agendamento ({@link Batch}, {@link BatchBuilder}) — os
 * tipos que só aparecem na assinatura da própria fachada. A identidade
 * compartilhada mora em subpacote à parte
 * ({@code io.mohs.core.job}: {@link io.mohs.core.job.JobKey},
 * {@link io.mohs.core.job.JobRef}) porque é usada por vários pacotes pares
 * (definition, execution, event) que não devem depender uns dos outros. O
 * restante do vocabulário público vive em subpacotes coesos:
 * {@code io.mohs.core.schedule}, {@code io.mohs.core.definition},
 * {@code io.mohs.core.execution}, {@code io.mohs.core.event},
 * {@code io.mohs.core.resource} (ver
 * {@code docs/adr/0015-consolidate-public-api-under-core.md}, que revisa
 * {@code docs/adr/0013-public-api-subpackaging.md}).
 *
 * <p>Tudo aqui e nos subpacotes acima é contrato — records, interfaces
 * seladas e interfaces simples, sem fiação de motor. A implementação real
 * vive em {@code io.mohs.engine} e {@code io.mohs.store.jdbc}, dos quais nenhum
 * tipo público pode depender (regra ArchUnit). {@code io.mohs.cron} é
 * utilitário à parte — não é vocabulário de job, não migrou pra cá.
 */
@NullMarked
package io.mohs.core;

import org.jspecify.annotations.NullMarked;
