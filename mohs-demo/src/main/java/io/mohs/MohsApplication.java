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
package io.mohs;

import java.util.Map;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.AutoConfigurationExcludeFilter;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.TypeExcludeFilter;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.FilterType;

/**
 * {@code @ComponentScan} excludes {@code io.mohs.rest}.
 *
 * <p>Without that, the default component scan (rooted at {@code io.mohs}) would find
 * {@code JobsController}/{@code ExecutionsController} on its own and collide with the explicit
 * {@code @Bean} in {@code MohsRestAutoConfiguration} whenever {@code mohs.api.enabled=true} — two
 * bean definitions for the same type. Excluding the package also makes this dev application
 * exercise exactly the auto-configuration path a real consumer takes; {@code main()} only lives
 * inside the library's own package as a development convenience.
 *
 * <p>{@code excludeFilters} repeats the two default filters of {@code @SpringBootApplication}
 * ({@link TypeExcludeFilter}, {@link AutoConfigurationExcludeFilter}) because declaring
 * {@code @ComponentScan} directly on the class REPLACES the meta-annotated one rather than adding
 * to it. Without them, {@code MohsAutoConfiguration} and {@code MohsRestAutoConfiguration} (both
 * under {@code io.mohs}) are found by the ordinary scan as well as through auto-configuration —
 * confirmed by a head-on collision with {@code @WebMvcTest} for any other controller (duplicate
 * bean, and {@code mohsClock} demanding a {@code DataSource} in a slice that should load no engine
 * at all).
 */
@SpringBootApplication
@ComponentScan(basePackages = "io.mohs", excludeFilters = {
        @ComponentScan.Filter(type = FilterType.CUSTOM, classes = TypeExcludeFilter.class),
        @ComponentScan.Filter(type = FilterType.CUSTOM, classes = AutoConfigurationExcludeFilter.class),
        @ComponentScan.Filter(type = FilterType.REGEX, pattern = "io\\.mohs\\.rest\\..*")
})
public class MohsApplication {

    /**
     * Local development configuration lives here, never in {@code src/main/resources/
     * application.yaml}: an {@code application.yaml} at the library jar's classpath root competes
     * with the host application's own — only one is loaded, decided by classpath order — and
     * application configuration always belongs to the application, never to a dependency.
     * {@code defaultProperties} loses to any external source (a developer's file, an argument, an
     * environment variable): it only fills the gap, and only when this {@code main()} is what
     * started the process.
     */
    public static void main(String[] args) {
        SpringApplication app = new SpringApplication(MohsApplication.class);
        // Mohs does not create its schema — the application does, and this demo is an application.
        // spring.sql.init is the ordinary Boot mechanism for it, and schema-h2.sql ships in
        // mohs-store-jdbc's jar, so a host reaches it on the classpath exactly like this. A real
        // deployment applies the file with its own tooling instead and leaves this off.
        app.setDefaultProperties(Map.of(
                "spring.application.name", "mohs",
                "mohs.jdbc.dialect", "h2",
                "spring.sql.init.mode", "always",
                "spring.sql.init.schema-locations", "classpath:schema-h2.sql"));
        app.run(args);
    }

}
