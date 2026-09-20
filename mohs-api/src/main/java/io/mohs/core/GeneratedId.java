/* Copyright 2026 The Mohs Authors. Licensed under the Apache License, Version 2.0. */
package io.mohs.core;

import java.util.Objects;

/**
 * Canonical UUIDv7 text shared by generated execution, batch, session and acquisition identities.
 */
public final class GeneratedId {

    private GeneratedId() {}

    /**
     * @param value the serialized identifier
     * @param name  the argument name for a validation failure
     * @return the unchanged canonical value
     */
    public static String require(String value, String name) {
        Objects.requireNonNull(value, name);

        if (value.length() != 36 || value.charAt(14) != '7' || "89ab".indexOf(value.charAt(19)) < 0) {
            throw invalid(name);
        }

        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (i == 8 || i == 13 || i == 18 || i == 23) {
                if (c != '-') throw invalid(name);
            } else if (!(c >= '0' && c <= '9' || c >= 'a' && c <= 'f')) {
                throw invalid(name);
            }
        }
        return value;
    }

    private static IllegalArgumentException invalid(String name) {
        return new IllegalArgumentException(name + " must be a canonical lowercase UUIDv7 (36 characters)");
    }
}
