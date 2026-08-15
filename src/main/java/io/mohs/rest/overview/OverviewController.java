package io.mohs.rest.overview;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import io.mohs.rest.ApiPaths;

/**
 * {@code GET /overview} — âncora de polling do dashboard (ver
 * {@code docs/REST-API-DESIGN.md}). Sem dependência de construtor: nada
 * aqui existe ainda pra consultar (M3); corpo stub por enquanto.
 */
@RestController
@RequestMapping("${mohs.api.base-path:" + ApiPaths.V1 + "}/overview")
public class OverviewController {

    @GetMapping
    public OverviewResponse overview() {
        throw new UnsupportedOperationException("M3: ainda não implementado");
    }
}
