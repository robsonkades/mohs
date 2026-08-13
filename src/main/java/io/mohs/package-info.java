/**
 * API pública do Mohs: definições de job, agendas, modelo de execução e a
 * fachada {@link io.mohs.Mohs}. Tudo aqui é contrato — records, interfaces
 * seladas e interfaces simples, sem fiação de motor. A implementação real
 * vive em {@code io.mohs.engine} e {@code io.mohs.jdbc}, dos quais este
 * pacote não pode depender.
 */
package io.mohs;
