package io.mohs.store.jdbc;

import java.sql.Types;
import java.time.LocalDateTime;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

/** Verifies the actual parameter metadata sent by the pinned driver, without changing column types. */
class SqlServerParameterBindingTest {
    @Test
    void pinnedDriverKeepsSevenDigitParameterMetadataEvenWithExplicitScale() throws Exception {
        try (var connection = SqlServerTestSupport.freshSchema().getConnection()) {
            var time = LocalDateTime.parse("2026-09-10T12:34:56.123456");
            String sql = "SELECT SQL_VARIANT_PROPERTY(?, 'BaseType'), SQL_VARIANT_PROPERTY(?, 'Scale'), ?";
            try (var defaultBinding = connection.prepareStatement(sql)) {
                for (int i = 1; i <= 3; i++) defaultBinding.setObject(i, time, Types.TIMESTAMP);
                try (var result = defaultBinding.executeQuery()) {
                    assertThat(result.next()).isTrue();
                    assertThat(result.getString(1)).isEqualTo("datetime2");
                    assertThat(result.getInt(2)).as("the default binding explains the measured scale conversion").isEqualTo(7);
                    assertThat(result.getObject(3, LocalDateTime.class)).isEqualTo(time);
                }
            }
            try (var exactBinding = connection.prepareStatement(sql)) {
                for (int i = 1; i <= 3; i++) exactBinding.setObject(i, time, Types.TIMESTAMP, 6);
                try (var result = exactBinding.executeQuery()) {
                    assertThat(result.next()).isTrue();
                    assertThat(result.getString(1)).isEqualTo("datetime2");
                    // Driver 13.4 Parameter.GetTypeDefinitionOp uses getDatetimeDataType(con, null)
                    // for ordinary, non-encrypted TIMESTAMP parameters. SQL Server defaults that to 7.
                    assertThat(result.getInt(2)).isEqualTo(7);
                    assertThat(result.getObject(3, LocalDateTime.class)).isEqualTo(time);
                }
            }
        }
    }
}
