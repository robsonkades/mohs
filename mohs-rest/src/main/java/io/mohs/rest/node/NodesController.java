package io.mohs.rest.node;

import java.util.List;
import java.util.Objects;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import io.mohs.core.Mohs;
import io.mohs.rest.ApiPaths;

/**
 * {@code GET /nodes} — visão de cluster: nodes com heartbeat recente,
 * last-seen, mais recente primeiro (a ordem vem de {@code Mohs#nodes}).
 * Morte não é campo da resposta: crash não escreve nada (ADR-0012) — o
 * leitor deriva vivo/suspeito da idade de {@code lastHeartbeatAt}, e
 * {@code STOPPED} é o único desfecho auto-reportado (shutdown limpo,
 * ADR-0041); o purge da mesma ADR limita a lista a nodes recentes.
 */
@RestController
@RequestMapping("${mohs.api.base-path:" + ApiPaths.V1 + "}/nodes")
public class NodesController {

    private final Mohs mohs;

    public NodesController(Mohs mohs) {
        this.mohs = Objects.requireNonNull(mohs, "mohs");
    }

    @GetMapping
    public List<NodeResponse> list() {
        return mohs.nodes().stream().map(NodeResponse::from).toList();
    }
}
