package io.mohs.rest.runner;

import java.util.List;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import io.mohs.rest.ApiPaths;

/** {@code GET /runners} — visão node-local: modo, max, em execução. Só leitura; runner é config, não runtime ajustável. */
@RestController
@RequestMapping(ApiPaths.V1 + "/runners")
public class RunnersController {

    @GetMapping
    public List<RunnerResponse> list() {
        throw new UnsupportedOperationException("M3: ainda não implementado");
    }
}
