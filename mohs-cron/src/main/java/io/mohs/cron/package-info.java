/**
 * Parsing e cálculo de próxima ocorrência de expressões cron seconds-first,
 * incluindo as extensões Quartz L/W/# — vendorizado de
 * {@code org.springframework.scheduling.support} (Spring Framework, Apache
 * License 2.0; ver o cabeçalho de cada arquivo para a atribuição exata e o
 * que foi adaptado). Motivo de vendorizar em vez de depender do Spring
 * diretamente: as classes de apoio ({@code CronField},
 * {@code BitsCronField}, {@code CompositeCronField}, {@code QuartzCronField})
 * são package-private no Spring — só {@code CronExpression} é público lá —
 * então usar o parser de verdade (não só a fachada) exige ter o código, não
 * só a dependência.
 *
 * <p>{@link io.mohs.cron.CronExpression} é utilitário autocontido — não
 * conhece {@link io.mohs.core.schedule.CronSpec}, {@link io.mohs.core.definition.JobDefinition}
 * nem nada do vocabulário de job. A costura entre a string crua de
 * {@code CronSpec.expression()} e o cálculo de próxima ocorrência é
 * responsabilidade do motor (M3), não deste pacote.
 */
@NullMarked
package io.mohs.cron;

import org.jspecify.annotations.NullMarked;
