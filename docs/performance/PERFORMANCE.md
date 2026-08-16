# Performance — guia de configuração e operação

Guia operacional de vazão do Mohs. Os números brutos e a metodologia vivem no
`BASELINE.md` (seção "Tuning fim a fim no Postgres", 2026-08-16) — este
documento explica **como raciocinar** sobre os knobs e como configurar um
node novo ou um cluster. Regra da casa: nenhum valor daqui é recomendação
universal; é ponto de partida para **medir no seu ambiente** (ver "Como
medir" no fim).

## O modelo mental

A vazão fim a fim é o mínimo de três estágios:

```
vazão = min( reabastecimento do claim , capacidade de dispatch , caminho de escrita no banco )
```

1. **Reabastecimento (tick)** — a cada `poll-interval`, o tick faz:
   heartbeat → renovação de leases em lote → reaper → triggers devidos →
   claim. Desde a ADR-0039, o claim pede `min(batch-size,
   dispatch-concurrency − in-flight)`: um node saturado **para de
   reivindicar** em vez de estourar o executor. O `poll-interval` usa
   `scheduleWithFixedDelay` — o intervalo conta **depois** que o tick
   termina; o período efetivo é `duração do tick + poll-interval`.
2. **Dispatch** — `dispatch-concurrency` virtual threads (runner `io`
   built-in). Cada execução paga: leitura de payload, `markFired`
   (1 transação), handler, transação de conclusão (UPDATE + INSERT de
   attempt). O custo por execução é dominado pelos **2 commits síncronos**
   (perfil medido: `LWLock:WALWrite` no topo), não pelo handler trivial.
3. **Banco** — commits/s e planos de execução. As duas lições medidas:
   índice parcial com predicado que um CAS por id implica captura o planner
   (DBTUNE-17, 8,4ms → 0,105ms por conclusão); e round trips por execução
   importam mais que CPU do banco (DBTUNE-16/18).

Vazão por node ≈ `dispatch-concurrency / latência por execução`. No bench
(handler trivial, Postgres local), a latência por execução ficou em
~130–250ms sob carga — por isso subir `dispatch-concurrency` foi a alavanca
final: 256 → 1024 levou de ~1.9k/s a ~4.2k/s.

## Os knobs, na ordem em que importam

| Propriedade | Default | Papel | Como dimensionar |
|---|---|---|---|
| `mohs.engine.dispatch-concurrency` | 64 | **Teto real do node** (ADR-0039): dimensiona o runner `io` E o clamp de claim | A alavanca principal de vazão. Virtual threads: subir é barato na JVM; o limite real vem do pool JDBC e do banco. Meça: se a vazão não subir junto, o gargalo já é o banco |
| `mohs.engine.poll-interval` | 5s | Latência de pickup e cadência de reabastecimento | 5s é default de *scheduler*, não de fila de alto volume. Para vazão: 50–100ms. Abaixo disso só compra ticks vazios em idle |
| `mohs.engine.batch-size` | 50 | Teto de claim por tick | ≈ `dispatch-concurrency` (o clamp nunca usa mais que a folga). Batch maior que a folga é inócuo; menor vira gargalo de reabastecimento |
| `mohs.engine.event-concurrency` | 16 | Executor de publicação de eventos | Sob vazão alta, 16 vira fila. Suba junto com o dispatch (bench usou 256 para 1024 de dispatch) |
| `spring.datasource.hikari.maximum-pool-size` | 10 | Conexões JDBC | Virtual threads pedem pool alto (CLAUDE.md: 100+) e `connection-timeout` baixo (<3s). **Nunca dimensione olhando só um node** — ver multi-instância abaixo |
| `mohs.engine.lease-ttl` | 30s | Detecção de morte de node | Menor = failover mais rápido, mas mais risco de reclaim falso sob stall (GC, CPU starvation). 30s é um bom equilíbrio; não encurte para "ganhar vazão" — lease não é knob de vazão |

**Ponto de operação medido nesta máquina** (node único, handler trivial,
Postgres 18 local em Docker): `poll-interval=50ms`, `batch-size=1000`,
`dispatch-concurrency=1024`, `event-concurrency=256`, Hikari 300 →
**4.0–4.2k execuções/s** sustentados, zero retry, zero rejeição. Use como
referência de forma, não como número portátil.

## Regras de coerência (violar = patologia conhecida)

- **Não sobrescreva o runner `io` com `max` menor que
  `dispatch-concurrency`.** O clamp segue `dispatch-concurrency`; um `io`
  menor reintroduz a rejeição no cano principal — execução fica RUNNING
  presa até o reaper e vira duplicata (foi exatamente a patologia medida:
  56k rejeições num drain de 50k). O boot emite WARN nomeando os dois
  valores; trate o WARN como erro de config.
- **`batch-size` alto com `dispatch-concurrency` baixo não acelera nada** —
  o clamp corta no menor. Suba os dois juntos.
- **Índices novos em `mohs_executions` exigem EXPLAIN dos statements do
  caminho quente** (claim, conclusão, renovação, reaper) antes de entrar —
  um índice parcial cujo predicado o CAS por id implica rouba o plano da PK
  (DBTUNE-17). O `ClaimQueryExplainHarness` existe para isso.
- **Handler não-trivial muda tudo.** O bench mede o overhead do motor.
  Handler de 100ms derruba a vazão por node para
  `dispatch-concurrency / 0,1s` — dimensione a partir da latência real do
  SEU handler, medida.

## Múltiplas instâncias (scale-out)

O claim é *Competing Consumers* sobre `FOR UPDATE SKIP LOCKED` + CAS
(ADR-0016/0018): nodes novos **não precisam de nenhuma configuração de
cluster** — apontar para o mesmo banco basta. Cada node reivindica com o
próprio clamp; dois nodes nunca executam o mesmo disparo (o CAS resolve; a
prioridade e a ordem `(priority, scheduled_at)` valem globalmente).

Ao adicionar instâncias, o que muda de verdade:

1. **O pool JDBC é POR NODE; o `max_connections` do Postgres é do
   cluster.** Regra de dimensionamento:

   ```
   hikari.maximum-pool-size ≤ (max_connections − reserva) / número de nodes
   ```

   Reserva de ~20 para ferramentas/replicação. Exemplo: `max_connections=500`,
   3 nodes → pool ≤ 160 por node. Estourar isso derruba nodes no boot com
   erro de conexão — a falha aparece no node novo, mas a causa é o cluster.
   (O bench de node único usou 300/500 — dois nodes nessa config já não
   cabem.)

2. **`dispatch-concurrency` é por node; a vazão do cluster NÃO é N × node.**
   O teto compartilhado é o caminho de commit do banco (WAL). Sinais de
   saturação: latência de commit subindo, `LWLock:WALWrite`/`IO:WalSync`
   dominando `pg_stat_activity`, vazão total estagnada ao adicionar node.
   A partir daí, node novo só adiciona redundância, não vazão — as
   alavancas passam a ser as do banco (propostas registradas no BASELINE:
   fusão do `markFired`, conclusão em lote, `synchronous_commit` por
   sessão, fillfactor).

3. **Comece o node novo igual aos existentes, com o pool recalculado.**
   Config heterogênea entre nodes funciona (cada clamp é local), mas
   complica atribuição de qualquer anomalia — prefira homogêneo e meça o
   cluster com a mesma janela de attempts do BASELINE.

4. **`poll-interval` não precisa encurtar com mais nodes** — N nodes já
   multiplicam a cadência agregada de claim. Encurtar em todos só aumenta
   ticks concorrentes disputando o `SKIP LOCKED` (barato, mas inútil).

5. **Failover é o reaper**: node que morre deixa seu in-flight expirar em
   `lease-ttl` e qualquer sobrevivente reclama com o orçamento de retry
   (garantia at-least-once exige `retries > 0` no job — o default 0 é
   at-most-once, ADR-0012/0033). `GET /nodes` (M3 pendente) é o
   observatório disso.

## Como medir (sempre, antes e depois de qualquer mudança)

Nunca reporte vazão por latência de cliente HTTP. O método do BASELINE:

1. Semeie backlog real (10k+ via REST ou INSERT direto com a forma de linha
   do REST — receita na seção DBTUNE-17/18 do BASELINE).
2. Meça pela janela de `mohs_attempts`:
   `count / (max(finished_at) − min(started_at))` do lote semeado.
   Atenção: `ENQUEUED = 0` significa tudo *reivindicado*, não concluído —
   espere o lote inteiro chegar a estado terminal.
3. Duas rodadas por configuração (variância medida: ~±10%).
4. Atribuição de gargalo: amostre `pg_stat_activity` (com wait events)
   DURANTE o drain; confirme retries/rejeições zerados
   (`attempts > 1` por execução e WARN de rejeição no log do node).

Toda mudança de vazão entra no `BASELINE.md` como seção nova, com
antes/depois e plano de execução arquivado — baseline não se edita
retroativamente.
