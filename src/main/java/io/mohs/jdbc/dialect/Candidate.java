package io.mohs.jdbc.dialect;

/** Linha crua de {@link JdbcDialect#selectCandidates} — {@code io.mohs.jdbc.JdbcClaimer} decide o resto. */
public record Candidate(String id, String jobKey, boolean allowConcurrentExecutions) {
}
