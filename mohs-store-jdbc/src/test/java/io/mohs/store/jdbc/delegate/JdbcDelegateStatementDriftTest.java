/*
 * Copyright 2026 The Mohs Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.mohs.store.jdbc.delegate;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The guard-rail under the duplication: every statement now exists FOUR times, once per delegate, and a
 * correction applied to three of them is a silent divergence — the H2 suite would stay green while
 * Postgres, MySQL or SQL Server ran the old text.
 *
 * <p>What it can check without a container is the CONTRACT the caller depends on: each delegate's
 * version of a statement binds exactly the same named parameters. That catches the realistic accident —
 * a predicate added, a column renamed, a filter dropped in one copy — because every one of those moves
 * a {@code :name}. It cannot catch a divergence that keeps the parameters identical (a changed
 * {@code ORDER BY}, a wrong table), and it is not meant to: the four delegates have four different
 * SQL SHAPES on purpose, so comparing the text itself would be a test that fails by design.
 *
 * <p>Reflection rather than an enumerated list, deliberately: a statement added to {@link JdbcDelegate}
 * enters this test by existing, which is the only way a guard over a growing interface stays honest.
 */
class JdbcDelegateStatementDriftTest {

    private static final List<JdbcDelegate> DELEGATES = List.of(
            new H2JdbcDelegate(), new PostgresJdbcDelegate(), new MySqlJdbcDelegate(), new SqlServerJdbcDelegate());

    /** A named parameter as {@code NamedParameterJdbcTemplate} parses it. */
    private static final Pattern NAMED_PARAMETER = Pattern.compile(":([a-zA-Z][a-zA-Z0-9_]*)");

    /** The filters {@code JdbcHistoryStore#findPage} assembles — the caller's half of the page statement. */
    private static final String SAMPLE_WHERE = "WHERE e.job_key = :jobKey AND e.execution_id < :cursor\n";

    /**
     * The one textual assertion this class allows itself, because parameters cannot see it: in the
     * subquery-shaped sweep delete (H2/PostgreSQL), the terminal-state guard must ALSO sit in the
     * OUTER {@code WHERE} — a predicate inside the subquery evaluates against a snapshot and
     * serialises nothing, so only the outer copy, re-evaluated under the row lock, spares a row a
     * concurrent manual retry just rearmed. The guard is made of literals, no {@code :param}, so the
     * parameter comparison above would wave through a well-meaning "dedup" that deletes rearmed
     * executions. The race itself is pinned against a real database by
     * {@code HistorySweepRearmRacePostgresTest}; this pins the H2 twin, which has no container test.
     */
    @Test
    void theSubqueryShapedSweepKeepsItsOuterTerminalGuard() {
        for (JdbcDelegate delegate : List.of(new H2JdbcDelegate(), new PostgresJdbcDelegate())) {
            String sql = delegate.pruneTerminalExecutionsBefore();
            // Everything after the subquery's LIMIT is the outer WHERE's tail — the guard lives there.
            assertThat(sql.substring(sql.indexOf("LIMIT :limit")))
                    .as("%s lost the OUTER terminal guard — only the outer predicate is re-evaluated under the row lock",
                            delegate.getClass().getSimpleName())
                    .contains("state IN");
        }
    }

    @Test
    void everyDelegateBindsTheSameParametersForTheSameStatement() {
        List<String> drift = new ArrayList<>();
        for (Statement statement : statements()) {
            JdbcDelegate reference = DELEGATES.getFirst();
            Set<String> expected = parametersOf(statement.textFrom(reference));
            for (JdbcDelegate delegate : DELEGATES.subList(1, DELEGATES.size())) {
                Set<String> actual = parametersOf(statement.textFrom(delegate));
                if (!actual.equals(expected)) {
                    drift.add("%s: %s binds %s, %s binds %s".formatted(statement.name(),
                            reference.getClass().getSimpleName(), expected,
                            delegate.getClass().getSimpleName(), actual));
                }
            }
        }

        assertThat(drift)
                .as("the same statement binds different named parameters across delegates — one copy was "
                        + "corrected and the others were not, and only the delegate with a container in CI "
                        + "would ever notice")
                .isEmpty();
    }

    @Test
    void everyDelegateAnswersEveryStatementWithSql() {
        List<String> empty = new ArrayList<>();
        for (Statement statement : statements()) {
            for (JdbcDelegate delegate : DELEGATES) {
                if (statement.textFrom(delegate).isBlank()) {
                    empty.add(delegate.getClass().getSimpleName() + "#" + statement.name());
                }
            }
        }

        assertThat(empty).as("a delegate answered a statement with blank SQL").isEmpty();
    }

    /** One statement of the interface, already bound to the arguments that make it callable. */
    private record Statement(String name, Object[] arguments, Method method) {

        String textFrom(JdbcDelegate delegate) {
            try {
                return (String) method.invoke(delegate, arguments);
            } catch (IllegalAccessException | InvocationTargetException e) {
                throw new IllegalStateException("could not read " + name + " from " + delegate.getClass(), e);
            }
        }
    }

    /**
     * Every {@code String}-returning method of the interface, with representative arguments for the two
     * that take any. A new parameterised statement fails HERE rather than being skipped in silence —
     * the alternative (ignoring what this method cannot bind) is a guard that quietly shrinks.
     */
    private static List<Statement> statements() {
        List<Statement> statements = new ArrayList<>();
        for (Method method : JdbcDelegate.class.getDeclaredMethods()) {
            if (method.getReturnType() != String.class) {
                continue;
            }
            switch (method.getParameterCount()) {
                case 0 -> statements.add(new Statement(method.getName(), new Object[0], method));
                case 1 -> {
                    Class<?> parameter = method.getParameterTypes()[0];
                    if (parameter == String.class) {
                        statements.add(new Statement(method.getName(), new Object[] { SAMPLE_WHERE }, method));
                    } else if (parameter == boolean.class) {
                        statements.add(new Statement(method.getName() + "(true)", new Object[] { true }, method));
                        statements.add(new Statement(method.getName() + "(false)", new Object[] { false }, method));
                    } else {
                        throw new IllegalStateException("no representative argument for " + method
                                + " — add one, or this statement escapes the drift guard");
                    }
                }
                default -> throw new IllegalStateException("no representative arguments for " + method
                        + " — add them, or this statement escapes the drift guard");
            }
        }
        // 66 statements plus the extra arm of upsertJobUpdate(boolean); the floor is what stops this
        // guard from turning into a tautology if the reflection ever stops finding what it guards
        assertThat(statements).as("the guard is only worth anything if it finds the statements it guards")
                .hasSizeGreaterThanOrEqualTo(67);
        return statements;
    }

    private static Set<String> parametersOf(String sql) {
        Set<String> parameters = new LinkedHashSet<>();
        Matcher matcher = NAMED_PARAMETER.matcher(sql);
        while (matcher.find()) {
            parameters.add(matcher.group(1));
        }
        return parameters;
    }

    /**
     * The invariant a static initialiser used to hold while the filtered claim was derived from the
     * unfiltered one by {@code replace}: the filter cannot silently disappear.
     *
     * <p>Both variants are written out in full now, so the drift is no longer a broken anchor but an
     * edit applied to one and not the other — which the cross-delegate check above cannot see, since it
     * compares a statement against its peers rather than against its own sibling. Postgres runs the
     * whole claim as one statement, SQL Server the queue half of it, and both reach this through their
     * two constants instead.
     */
    @Test
    void theFilteredClaimNeverLosesItsInadmissiblePredicate() {
        for (JdbcDelegate delegate : DELEGATES) {
            assertThat(parametersOf(delegate.readyCandidatesFiltered()))
                    .as("%s#readyCandidatesFiltered lost the predicate that excludes inadmissible jobs — "
                            + "the claim would hand back work admission control had already refused",
                            delegate.getClass().getSimpleName())
                    .contains("inadmissible");
            assertThat(parametersOf(delegate.readyCandidates()))
                    .as("%s#readyCandidates binds :inadmissible, but a NOT IN over an empty list does not "
                            + "expand — that is the whole reason the two statements are separate",
                            delegate.getClass().getSimpleName())
                    .doesNotContain("inadmissible");
        }
        assertThat(parametersOf(PostgresJdbcDelegate.CLAIM_READY_FILTERED)).contains("inadmissible");
        assertThat(parametersOf(PostgresJdbcDelegate.CLAIM_READY)).doesNotContain("inadmissible");
        assertThat(parametersOf(SqlServerJdbcDelegate.CLAIM_READY_FILTERED)).contains("inadmissible");
        assertThat(parametersOf(SqlServerJdbcDelegate.CLAIM_READY)).doesNotContain("inadmissible");
    }

    /**
     * A folded claim and the pick it folds must bind alike: the portable text stays as the record of
     * what the fold replaces, and an edit to one that is not an edit to the other is a drift the
     * cross-delegate comparison cannot see (it compares peers, never a delegate with itself).
     */
    @Test
    void aFoldedClaimBindsWhatThePickItReplacesBinds() {
        SqlServerJdbcDelegate sqlServer = new SqlServerJdbcDelegate();
        assertThat(parametersOf(SqlServerJdbcDelegate.CLAIM_READY)).isEqualTo(parametersOf(sqlServer.readyCandidates()));
        assertThat(parametersOf(SqlServerJdbcDelegate.CLAIM_READY_FILTERED)).isEqualTo(parametersOf(sqlServer.readyCandidatesFiltered()));
        // Postgres folds the lease insert too, so its fold binds the pick's parameters plus the lease's
        PostgresJdbcDelegate postgres = new PostgresJdbcDelegate();
        assertThat(parametersOf(PostgresJdbcDelegate.CLAIM_READY)).containsAll(parametersOf(postgres.readyCandidates()));
        assertThat(parametersOf(PostgresJdbcDelegate.CLAIM_READY_FILTERED)).containsAll(parametersOf(postgres.readyCandidatesFiltered()));
    }

}
