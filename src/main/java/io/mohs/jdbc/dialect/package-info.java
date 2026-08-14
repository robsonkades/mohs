/**
 * As poucas divergências reais de dialeto SQL entre os bancos suportados
 * (ADR-0023) — modelo na forma do {@code LimitHandler}/{@code
 * LockingStrategy} do Hibernate (interfaces pequenas, uma preocupação
 * cada), sem trazer o Hibernate como dependência. {@link
 * io.mohs.jdbc.dialect.JdbcDialect} isola a única consulta sensível a
 * dialeto de {@code io.mohs.jdbc} ({@code SELECT} de candidatos ao
 * claim: posição do {@code LIMIT}/{@code TOP}, {@code SKIP LOCKED} vs.
 * hint de tabela) — cada banco suportado (H2, PostgreSQL, MySQL, SQL
 * Server) tem sua própria implementação, mesmo onde o SQL é
 * idêntico hoje, para não acoplar bancos independentes a uma
 * coincidência de sintaxe atual. Escolha explícita de qual {@link
 * io.mohs.jdbc.dialect.JdbcDialect} usar, nunca auto-detecção — quem
 * monta {@link io.mohs.jdbc.JdbcClaimer} decide.
 */
@NullMarked
package io.mohs.jdbc.dialect;

import org.jspecify.annotations.NullMarked;
