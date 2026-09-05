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
package io.mohs.rest;

import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;

/**
 * The {@code @SpringBootConfiguration} this module's {@code @WebMvcTest} slices find when walking up
 * from their own package. In the single-module days that role was played by
 * {@code io.mohs.MohsApplication}, which now lives in mohs-demo — a module this one does not see, and
 * should not.
 *
 * <p>Deliberately without a {@code @ComponentScan}: each contract test registers its controller
 * through an explicit {@code @Bean}, and a scan of {@code io.mohs.rest} would give two bean
 * definitions for the same type. It is the same exclusion {@code MohsApplication} had to declare by
 * hand — here it is the default, by absence.
 */
@SpringBootConfiguration
@EnableAutoConfiguration
class RestSliceConfiguration {
}
