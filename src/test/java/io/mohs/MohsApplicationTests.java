package io.mohs;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

/** {@code mohs.enabled=false}: este smoke test só confirma o bootstrap Spring Boot puro — wiring do motor tem sua própria suíte, {@code MohsAutoConfigurationTest}, com {@code mohs.jdbc.dialect} de verdade. */
@SpringBootTest(properties = "mohs.enabled=false")
class MohsApplicationTests {

    @Test
    void contextLoads() {
    }

}
