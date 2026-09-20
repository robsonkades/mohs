package io.mohs.core;

/**
 * Explicit input for handlers that receive only context or no arguments.
 */
public record NoInput() {
    public static final NoInput INSTANCE = new NoInput();
}
