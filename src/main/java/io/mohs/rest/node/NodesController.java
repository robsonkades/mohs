package io.mohs.rest.node;

import java.util.List;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/** {@code GET /nodes} — visão de cluster: nodes com heartbeat recente, last-seen. */
@RestController
@RequestMapping("/api/mohs/v1/nodes")
public class NodesController {

    @GetMapping
    public List<NodeResponse> list() {
        throw new UnsupportedOperationException("M3: ainda não implementado");
    }
}
