package io.mohs.store.jdbc;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.sql.DataSource;
import org.springframework.jdbc.datasource.AbstractDataSource;

/** Executes the real ready INSERT batch, then loses its response before the enclosing commit. */
final class TestJdbcFaults extends AbstractDataSource {
    private final DataSource delegate;
    private final String statementPrefix;
    private final AtomicBoolean failNext = new AtomicBoolean(true);

    TestJdbcFaults(DataSource delegate) { this(delegate, "INSERT INTO mohs_work"); }

    TestJdbcFaults(DataSource delegate, String statementPrefix) {
        this.delegate = delegate;
        this.statementPrefix = statementPrefix;
    }

    @Override
    public Connection getConnection() throws SQLException { return wrap(delegate.getConnection()); }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        return wrap(delegate.getConnection(username, password));
    }

    private Connection wrap(Connection connection) {
        return (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(), new Class<?>[]{Connection.class},
                (_, method, args) -> {
            Object result;
            try { result = method.invoke(connection, args); }
            catch (InvocationTargetException failure) { throw failure.getCause(); }
            if (method.getName().equals("prepareStatement") && args[0] instanceof String sql
                    && sql.stripLeading().startsWith(statementPrefix)) {
                var statement = (PreparedStatement) result;
                return Proxy.newProxyInstance(PreparedStatement.class.getClassLoader(), new Class<?>[]{PreparedStatement.class},
                        (_, operation, parameters) -> {
                    Object response;
                    try { response = operation.invoke(statement, parameters); }
                    catch (InvocationTargetException failure) { throw failure.getCause(); }
                    if (operation.getName().equals("executeBatch") && failNext.compareAndSet(true, false)) {
                        throw new SQLException("queue response lost before commit", "08006");
                    }
                    return response;
                });
            }
            return result;
        });
    }
}
