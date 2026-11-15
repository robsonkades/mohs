/* Copyright 2026 The Mohs Authors. Licensed under the Apache License, Version 2.0. */
package io.mohs.engine;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Map;
import java.util.TreeMap;
import io.mohs.core.TechnicalKey;
import io.mohs.core.job.JobKey;

/** Immutable wire contracts of this process. Java class names and handler implementations stay local. */
public record ExecutableCatalog(Map<JobKey, String> contracts) {
    public ExecutableCatalog {
        contracts = Map.copyOf(contracts);
        contracts.values().forEach(value -> TechnicalKey.require(value, "inputContract"));
    }

    /** Validates the complete publication before any retained definition can change. */
    public void requireDefinitions(java.util.List<io.mohs.core.definition.JobDefinition> definitions) {
        var keys = new java.util.HashSet<JobKey>();
        for (var definition : definitions) {
            if (!definition.key().equals(definition.executableKey()) || !keys.add(definition.key())) {
                throw new IllegalArgumentException("catalog definitions must be distinct self-bound executables");
            }
            require(definition.key(), definition.inputContract());
        }
        if (!keys.equals(contracts.keySet())) {
            throw new IllegalArgumentException("catalog and executable definitions differ");
        }
    }

    /** Stable SHA-256 over sorted, unambiguously separated ASCII identities. */
    public String fingerprint() {
        try {
            var digest = MessageDigest.getInstance("SHA-256");
            var sorted = new TreeMap<String, String>();
            contracts.forEach((key, value) -> sorted.put(key.value(), value));
            sorted.forEach((key, value) -> {
                digest.update(key.getBytes(StandardCharsets.US_ASCII));
                digest.update((byte) 0);
                digest.update(value.getBytes(StandardCharsets.US_ASCII));
                digest.update((byte) 0);
            });
            return HexFormat.of().formatHex(digest.digest());
        } catch (NoSuchAlgorithmException impossible) {
            throw new AssertionError(impossible);
        }
    }

    /** Fails before registration or acquisition if a required executable is absent or incompatible. */
    public void require(JobKey executable, String inputContract) {
        if (!inputContract.equals(contracts.get(executable))) {
            throw new IllegalStateException("catalog requires executable '" + executable.value()
                    + "' with input contract '" + inputContract + "' on every worker");
        }
    }
}
