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
package io.mohs.store.jdbc;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.SQLException;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;
import javax.sql.DataSource;
import org.springframework.jdbc.datasource.DelegatingDataSource;

/** Opt-in laboratory instrumentation. Counts JDBC execute calls, not server wire round trips. */
final class PerformanceDataSource extends DelegatingDataSource {
    private final LongAdder executions = new LongAdder();
    private final LongAdder borrows = new LongAdder();
    private final LongAdder borrowNanos = new LongAdder();
    private final ConcurrentHashMap<String, LongAdder> statements = new ConcurrentHashMap<>();

    PerformanceDataSource(DataSource delegate) { super(delegate); }

    record Snapshot(long executions, long borrows, long borrowNanos, Map<String, Long> statements) {}

    Snapshot snapshot() {
        var sql = new TreeMap<String, Long>();
        statements.forEach((key, value) -> sql.put(key, value.sum()));
        return new Snapshot(executions.sum(), borrows.sum(), borrowNanos.sum(), sql);
    }

    @Override public Connection getConnection() throws SQLException {
        long start = System.nanoTime();
        Connection connection = super.getConnection();
        borrowNanos.add(System.nanoTime() - start);
        borrows.increment();
        return wrap(connection);
    }

    private Connection wrap(Connection connection) {
        return (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(), new Class<?>[] {Connection.class},
                (proxy, method, args) -> {
                    Object result = invoke(connection, method, args);
                    if (result instanceof Statement statement && method.getName().startsWith("prepare")) {
                        return wrapStatement(statement, (String) args[0]);
                    }
                    if (result instanceof Statement statement && method.getName().equals("createStatement")) {
                        return wrapStatement(statement, "<dynamic>");
                    }
                    return result;
                });
    }

    private Object wrapStatement(Statement statement, String sql) {
        Class<?> api = statement instanceof PreparedStatement ? PreparedStatement.class : Statement.class;
        String normalized = sql.replaceAll("\\s+", " ").strip();
        return Proxy.newProxyInstance(Statement.class.getClassLoader(), new Class<?>[] {api},
                (proxy, method, args) -> {
                    if (method.getName().startsWith("execute")) {
                        executions.increment();
                        String actual = args != null && args.length > 0 && args[0] instanceof String text
                                ? text.replaceAll("\\s+", " ").strip() : normalized;
                        statements.computeIfAbsent(actual, _ -> new LongAdder()).increment();
                    }
                    return invoke(statement, method, args);
                });
    }

    private static Object invoke(Object target, Method method, Object[] args) throws Throwable {
        try { return method.invoke(target, args); }
        catch (InvocationTargetException failure) { throw failure.getCause(); }
    }
}
