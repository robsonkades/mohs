package io.mohs.rest.resource;

import java.util.List;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/** {@code GET /runners} — visão node-local: modo, max, em execução. Só leitura; runner é config, não runtime ajustável. */
@RestController
@RequestMapping("/api/mohs/v1/runners")
public class RunnersController {

    @GetMapping
    public List<RunnerResponse> list() {
        throw new UnsupportedOperationException("M3: ainda não implementado");
    }
}
