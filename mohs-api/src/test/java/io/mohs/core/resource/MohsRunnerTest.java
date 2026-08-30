/*
 * Copyright 2026 The Mohs Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.mohs.core.resource;

import org.junit.jupiter.api.Test;

import java.time.Duration;

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
    void maxConcurrentOverridesTheDefault() {
        MohsRunner runner = MohsRunner.io("s3").maxConcurrent(32).build();

        assertThat(runner.maxConcurrent()).isEqualTo(32);
    }

    @Test
    void rejectsNonPositiveMaxConcurrent() {
        assertThatThrownBy(() -> MohsRunner.io("s3").maxConcurrent(0).build())
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void cpuDefaultsCoreAndMaxSizeToAvailableProcessors() {
        MohsRunner runner = MohsRunner.cpu("crunch").build();

        assertThat(runner.mode()).isEqualTo(RunnerMode.CPU);
        assertThat(runner.coreSize()).isEqualTo(Runtime.getRuntime().availableProcessors());
        assertThat(runner.maxSize()).isEqualTo(Runtime.getRuntime().availableProcessors());
    }

    @Test
    void cpuQueueCapacityDefaultsToZero() {
        MohsRunner runner = MohsRunner.cpu("crunch").build();

        assertThat(runner.queueCapacity()).isZero();
    }

    @Test
    void cpuKeepAliveDefaultsToSixtySeconds() {
        MohsRunner runner = MohsRunner.cpu("crunch").build();

        assertThat(runner.keepAlive()).isEqualTo(Duration.ofSeconds(60));
    }

    @Test
    void cpuPropertiesOverrideTheDefaults() {
        MohsRunner runner = MohsRunner.cpu("crunch")
                .coreSize(4)
                .maxSize(8)
                .queueCapacity(100)
                .keepAlive(Duration.ofSeconds(30))
                .build();

        assertThat(runner.coreSize()).isEqualTo(4);
        assertThat(runner.maxSize()).isEqualTo(8);
        assertThat(runner.queueCapacity()).isEqualTo(100);
        assertThat(runner.keepAlive()).isEqualTo(Duration.ofSeconds(30));
    }

    @Test
    void rejectsCpuCoreSizeBelowOne() {
        assertThatThrownBy(() -> MohsRunner.cpu("crunch").coreSize(0).build())
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsCpuMaxSizeBelowCoreSize() {
        assertThatThrownBy(() -> MohsRunner.cpu("crunch").coreSize(4).maxSize(2).build())
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsCpuNegativeQueueCapacity() {
        assertThatThrownBy(() -> MohsRunner.cpu("crunch").queueCapacity(-1).build())
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsCpuNegativeKeepAlive() {
        assertThatThrownBy(() -> MohsRunner.cpu("crunch").keepAlive(Duration.ofSeconds(-1)).build())
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void canonicalConstructorRejectsCpuFieldsSetOnAnIoRunner() {
        assertThatThrownBy(() -> new MohsRunner("s3", RunnerMode.IO, 5, 4, 4, 0, Duration.ofSeconds(60)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void canonicalConstructorRejectsMaxConcurrentSetOnACpuRunner() {
        assertThatThrownBy(() -> new MohsRunner("crunch", RunnerMode.CPU, 5, 4, 4, 0, Duration.ofSeconds(60)))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
