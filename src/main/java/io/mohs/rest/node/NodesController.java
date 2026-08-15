package io.mohs.rest.node;

import java.util.List;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import io.mohs.rest.ApiPaths;

/** {@code GET /nodes} — visão de cluster: nodes com heartbeat recente, last-seen. */
@RestController
@RequestMapping("${mohs.api.base-path:" + ApiPaths.V1 + "}/nodes")
public class NodesController {

    @GetMapping
    public List<NodeResponse> list() {
        throw new UnsupportedOperationException("M3: ainda não implementado");
    }
}
