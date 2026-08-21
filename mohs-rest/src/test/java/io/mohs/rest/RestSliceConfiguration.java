package io.mohs.rest;

import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;

/**
 * A {@code @SpringBootConfiguration} que os {@code @WebMvcTest} deste módulo encontram ao
 * subir do próprio pacote. No módulo único era {@code io.mohs.MohsApplication} que fazia esse
 * papel; ela agora mora em mohs-demo, que este módulo não enxerga — e nem deveria.
 *
 * <p>Deliberadamente sem {@code @ComponentScan}: cada teste de contrato registra o seu
 * controller por {@code @Bean} explícito, e um scan de {@code io.mohs.rest} daria dois bean
 * definitions para o mesmo tipo. É a mesma exclusão que {@code MohsApplication} precisava
 * declarar à mão — aqui ela é o default, por ausência.
 */
@SpringBootConfiguration
@EnableAutoConfiguration
class RestSliceConfiguration {
}
