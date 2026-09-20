package io.mohs.core;

import java.util.Objects;
import java.util.regex.Pattern;

/**
 * Exact, case-sensitive technical identity. Human labels and audit actors are not technical keys.
 */
public final class TechnicalKey {

    public static final int MAX_LENGTH = 255;
    private static final Pattern ALLOWED = Pattern.compile("[A-Za-z0-9._:/-]+");

    private TechnicalKey() {}

    public static String require(String value, String field) {
        Objects.requireNonNull(value, field);
        if (value.isEmpty() || value.length() > MAX_LENGTH || !ALLOWED.matcher(value).matches()) {
            throw new IllegalArgumentException(field + " must contain 1 through " + MAX_LENGTH + " ASCII characters from A-Z, a-z, 0-9, dot, underscore, colon, slash or hyphen; spaces are not allowed (length=" + value.length() + ")");
        }
        return value;
    }
}
