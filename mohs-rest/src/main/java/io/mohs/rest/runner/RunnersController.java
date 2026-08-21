package io.mohs.rest.runner;

import java.util.List;
import java.util.Objects;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import io.mohs.core.Mohs;
import io.mohs.rest.ApiPaths;

/**
 * {@code GET /runners} — visão node-local: modo, max, em execução. Só
 * leitura; runner é config, não runtime ajustável.
 *
 * <p>Sem cursor, ao contrário das listagens de execução: a cardinalidade é o
 * que a aplicação declarou no boot, não o que ela acumulou rodando — mesmo
 * critério de {@code GET /jobs} e {@code GET /nodes}.
 *
 * <p>Node-local significa node-local: a resposta descreve o processo que
 * atendeu a requisição, não o cluster. Atrás de um load balancer, duas
 * chamadas seguidas podem legitimamente responder números diferentes — pool
 * de threads não é estado compartilhado.
 */
@RestController
@RequestMapping("${mohs.api.base-path:" + ApiPaths.V1 + "}/runners")
public class RunnersController {

    private final Mohs mohs;

    public RunnersController(Mohs mohs) {
        this.mohs = Objects.requireNonNull(mohs, "mohs");
    }

    @GetMapping
    public List<RunnerResponse> list() {
        return mohs.runners().stream().map(RunnerResponse::from).toList();
    }
}
