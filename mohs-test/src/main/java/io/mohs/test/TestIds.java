/* Copyright 2026 The Mohs Authors. Licensed under the Apache License, Version 2.0. */
package io.mohs.test;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

/** Deterministic UUIDv7-shaped identities for fixtures; these carry no real creation time. */
public final class TestIds {
    private TestIds() {}

    /** Returns an existing canonical identity unchanged, or a stable identity for a fixture label. */
    public static String value(String label) {
        if (label.matches("[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}")) return label;
        UUID hash = UUID.nameUUIDFromBytes(label.getBytes(StandardCharsets.UTF_8));
        return new UUID((hash.getMostSignificantBits() & ~0xf000L) | 0x7000L,
                (hash.getLeastSignificantBits() & 0x3fffffffffffffffL) | 0x8000000000000000L).toString();
    }
}
