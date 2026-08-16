# ADR-0041: Heartbeat final no stop e purge de linhas de node velhas

## Status
Decided — 2026-08-16. Origem: relato do autor ("nodes sempre ficam RUNNING no
banco") — 51 linhas em `mohs_nodes`, todas RUNNING, uma só de instância viva;
o resto era resíduo dos boots/kills do ciclo de bench.

## Context
`mohs_nodes` é informativo (ADR-0012): cada instância gera um `node_id` novo
(UUIDv7) no boot e faz upsert do heartbeat a cada tick com seu `EngineState`.
Morte é derivada na LEITURA pela staleness do heartbeat — crash não escreve
nada, e isso é design, não lacuna. Mas duas lacunas reais existiam:

1. Nem o shutdown GRACIOSO escrevia um estado final — `stop()` cancelava o
   tick e pronto; a linha parava no último estado tickado (RUNNING/DRAINING)
   para sempre. Stop limpo e crash ficavam indistinguíveis no banco.
2. Nada removia linha velha — como o `node_id` nunca se repete, a tabela só
   cresce: uma linha órfã por boot, para sempre.

## Decision
- **Heartbeat final:** `Engine.stop()` grava um último heartbeat `STOPPED`,
  best-effort (try/catch com WARN — shutdown nunca falha por banco fora;
  quem não consegue escrever fica coberto pela staleness + purge).
- **Purge de carona no tick** (como o reaper): heartbeats estritamente mais
  velhos que `lease-ttl × 10` são deletados. Retention derivada do TTL
  existente — nenhum knob novo (CLAUDE.md); 10 leases (5 min no default)
  mantêm o node morto visível como stale por tempo de sobra para
  `GET /nodes`/alertas antes de virar lixo. INFO com contagem quando >0.
- O que NÃO muda: morte continua derivada na leitura (ADR-0012). O purge
  não é failure detection — só recolhe o que nenhum leitor tem mais uso.

## Consequences
- `GET /nodes` (M3 pendente) lê uma tabela honesta: vivos (heartbeat
  recente), parados de forma limpa (`STOPPED`), suspeitos (stale dentro da
  retention) — e nada de arqueologia de boots antigos.
- Cluster inteiro desligado não purga nada (ninguém ticka) — o primeiro
  boot seguinte recolhe; aceito.
- Custo: um DELETE por tick numa tabela do tamanho do cluster + resíduo
  recente — desprezível; se um dia pesar, medir antes de mexer (BASELINE).
- A janela "morreu → linha some" é fixa em 10 leases; leitor que quiser
  histórico mais longo de nodes mortos tem um caso de uso novo — aí sim
  nasce configuração, com número (YAGNI até lá).
