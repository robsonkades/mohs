package io.mohs.resource;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MohsRunnerTest {

    @Test
    void ioDefaultsToSixtyFour() {
        MohsRunner runner = MohsRunner.io("s3").build();

        assertThat(runner.name()).isEqualTo("s3");
        assertThat(runner.mode()).isEqualTo(RunnerMode.IO);
        assertThat(runner.maxConcurrent()).isEqualTo(64);
    }

    @Test
    void cpuDefaultsToAvailableProcessors() {
        MohsRunner runner = MohsRunner.cpu("crunch").build();

        assertThat(runner.mode()).isEqualTo(RunnerMode.CPU);
        assertThat(runner.maxConcurrent()).isEqualTo(Runtime.getRuntime().availableProcessors());
    }

    @Test
    void maxConcurrentOverridesTheDefault() {
        MohsRunner runner = MohsRunner.io("s3").maxConcurrent(32).build();

        assertThat(runner.maxConcurrent()).isEqualTo(32);
    }

    @Test
    void rejectsNonPositiveMaxConcurrent() {
        assertThatThrownBy(() -> MohsRunner.io("s3").maxConcurrent(0).build())
                .isInstanceOf(IllegalArgumentException.class);
    }
}
