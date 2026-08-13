/**
 * Public API of Mohs: job definitions, schedules, execution model, and the
 * {@link io.mohs.Mohs} facade. Everything here is a contract — records,
 * sealed interfaces, and plain interfaces with no engine wiring. The
 * runtime implementation lives in {@code io.mohs.engine} and
 * {@code io.mohs.jdbc}, neither of which this package may depend on.
 */
package io.mohs;
