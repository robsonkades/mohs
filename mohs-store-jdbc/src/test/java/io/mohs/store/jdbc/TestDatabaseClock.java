package io.mohs.store.jdbc;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.time.Clock;
import io.mohs.store.jdbc.delegate.JdbcDelegate;

/** Controls only the database-time observation in deterministic fixtures; production never uses this. */
public final class TestDatabaseClock {
    private TestDatabaseClock() {}

    public static JdbcDelegate at(JdbcDelegate delegate, Clock clock) {
        return (JdbcDelegate) Proxy.newProxyInstance(delegate.getClass().getClassLoader(), new Class<?>[]{JdbcDelegate.class},
                (_, method, args) -> {
                    if (method.getName().equals("readNow")) return clock.instant();
                    try { return method.invoke(delegate, args); }
                    catch (InvocationTargetException failure) { throw failure.getCause(); }
                });
    }
}
