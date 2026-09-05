Achados vetados, por alavancagem

Atualização de 2026-09-05: ajustes dos itens 19, 21, 25 e 35 implementados, validados e commitados localmente, sem push.

- 19: no excedente do filtro, requeue adia a visibilidade por `max-poll-interval`, preservando a tentativa.
- 21: validação de `connection-timeout < node-lease-ttl` para Hikari direto e proxies delegadores Spring; outros pools exigem conferência operacional.
- 25: amostras com RTT acima de 1 s são rejeitadas; offsets podem diminuir, com monotonicidade nos instantes retornados.
- 35: conversão de payload não expõe mensagens Jackson/classes/valores; erros de recurso ausente não refletem identificadores; detalhes de construtor restritos aos requests Mohs.

Validação de 2026-09-05, após liberar as permissões, usando Temurin 25.0.3:

- `mvnw.cmd test -pl mohs-spring-boot-starter -am -Dtest=DispatcherTest,EngineTest,DatabaseClockTest,RestExceptionHandlerTest,MohsAutoConfigurationTest -Dsurefire.failIfNoSpecifiedTests=false -Dskip.frontend=true`: BUILD SUCCESS; 139 testes, zero falhas, erros ou ignorados.
- `mvnw.cmd test -Dskip.frontend=true`: BUILD SUCCESS; 860 testes em 105 classes, zero falhas, erros ou ignorados. Inclui Testcontainers com PostgreSQL, MySQL e SQL Server.
- O mesmo comando passou novamente em uma exportação limpa do commit `dad2b0c`, em `C:\git\mohs-validation-20260905`: 860 testes em 105 classes, zero falhas, erros ou ignorados. Essa execução confirma que o código commitado funciona sem depender das refatorações de `Engine` mantidas fora dos commits.
- `mvnw.cmd verify -DskipTests`: BUILD SUCCESS, incluindo empacotamento, Javadoc e frontend (`npm ci`, TypeScript e Vite). Os testes não foram repetidos nessa etapa.
- `git diff --check`: passou. Revisões de refatoração, persistência e REST/starter: nenhum defeito de código bloqueador encontrado; nenhuma refatoração adicional nem tuning aplicado nesta validação.

Conclusão após aprovação das duas pendências pelo usuário:

- As extrações preexistentes de métodos em `Engine` foram incluídas em commit separado. O SHA-256 do arquivo corresponde exatamente ao código que passou nos 860 testes e no `verify` acima; não houve nova alteração de Java nem repetição desses testes.
- O trecho sobre CSRF em `docs/08-security/security-overview.md` foi corrigido: o exemplo mantém a proteção padrão, inclui HTTP Basic e mTLS de navegador entre as credenciais automáticas e exige proteção na entrada do gateway quando ele troca cookie por bearer. Referências: [Spring Security](https://docs.spring.io/spring-security/reference/servlet/exploits/csrf.html) e [OWASP](https://owasp.org/www-community/attacks/csrf).

Correções adicionais autorizadas e validadas em 2026-09-05:

- `ClusterEngineTest` agora espera que cada engine observe os dois nós RUNNING, oferece uma execução por shard usando `Shards.of` e exige os dois IDs nos attempts concluídos. Os dois testes da classe passaram, incluindo recuperação de nó particionado; `mvnw.cmd test -pl mohs-store-jdbc -am -Dtest=ClusterEngineTest -Dsurefire.failIfNoSpecifiedTests=false`: BUILD SUCCESS.
- O lockfile atualizou `fast-uri` de 3.1.5 para 3.1.7 e `qs` de 6.15.3 para 6.16.0, sem mudar dependências diretas. `npm audit fix --package-lock-only --ignore-scripts` e o `npm ci` subsequente informaram zero vulnerabilidades; o build TypeScript/Vite passou.
- As referências inválidas de Javadoc em `NextFireCalculator` e no package-info do demo agora apontam para classes existentes. `mvnw.cmd verify -DskipTests` passou. A documentação do Engine foi regenerada após remover apenas seu arquivo de cache de Javadoc, e uma varredura dos HTMLs gerados de todos os módulos não encontrou marcadores `invalid-tag`.

Permanece o inventário de comentários e tags de Javadoc ausentes. Esses avisos não foram desativados; nesta etapa foi corrigido o conteúdo inválido, conforme o recorte comunicado ao usuário.

O inventário original abaixo foi preservado; seus achados não são uma lista de correções aprovadas ou uma certificação do estado atual.

Legenda: esforço S horas, M um dia, L vários dias. Confiança alta salvo nota.

Bloqueiam o release 0.1.0

#: 1
Achado: O test-jar de mohs-store-jdbc vai para o Central e para o GitHub release
Cat: deps
Impacto: Publica *TestSupport e um split package io.mohs.engine; Central não despublica
Esf: S
Risco: LOW
Evidência: mohs-store-jdbc/pom.xml:97, release.yml:96,108
────────────────────────────────────────
#: 2
Achado: Config de produção recomendada traz mohs.jdbc.migrate: true, propriedade que não existe
Cat: docs
Impacto: Boot silencioso, schema nunca aplicado, falha no primeiro write
Esf: S
Risco: LOW
Evidência: configuration-reference.md:78
────────────────────────────────────────
#: 3
Achado: 13 trechos de docs usam groupId io.mohs; o real é io.github.robsonkades
Cat: docs
Impacto: O snippet de instalação não resolve
Esf: S
Risco: LOW
Evidência: modules.md:61-84, module-architecture.md:179-203
────────────────────────────────────────
#: 4
Achado: Docs marcam como ausentes 4 features entregues: health indicator, @OnExecution, retryPolicy, retenção de histórico. 9
locais, inclusive mensagem de boot e grep de runbook que nunca casam
Cat: docs
Impacto: Integrador reimplementa o que existe; operador procura erro inexistente
Esf: M
Risco: LOW
Evidência: capabilities.md:21,22,92,105, troubleshooting.md:16, runbook.md:234
────────────────────────────────────────
#: 5
Achado: 18 arquivos afirmam "doze regras ArchUnncia versão morta; Javadoc publicado de
io.mohs.core repete a alegação
Cat: docs
Impacto: Contribuidor acredita que Instant.now() é barrado; não é
Esf: M
Risco: LOW
Evidência: README.md:123, CONTRIBUTING.md:43, pom.xml:71,129
────────────────────────────────────────
#: 6
Achado: CLAUDE.md aponta 11 caminhos inexistentes, cita BASELINE.md como autoridade 4 vezes, 10 ADRs por número e 2 trechos
em português
Cat: dx
Impacto: Todo agente começa a sessão com mapa falso
Esf: S
Risco: MED
Evidência: CLAUDE.md:32,51,87,126-147,166
────────────────────────────────────────
#: 7
Achado: CHANGELOG sem history-retention, sem ex e sem o delta V9
Cat: docs
Impacto: Operador não sabe do ALTER DATABASE ne
Esf: S
Risco: LOW
Evidência: git log e6667fb..HEAD vs CHANGELOG.m
────────────────────────────────────────
#: 8
Achado: Sete comentários em português em 4 .java, dois deles nas anotações do quick start; CLAUDE.md e ledger afirmam "Java
is done"
Cat: docs
Impacto: Vai para o javadoc.io do 0.1.0
Esf: S
Risco: LOW
Evidência: OnDemandJob.java:50,74,78, Recurring:237, MohsImpl.java:185
────────────────────────────────────────
#: 9
Achado: Release workflow: chave GPG importada aag mutável, sem bump do outputTimestamp, sem
volta para -SNAPSHOT
Cat: security
Impacto: Supply chain do artefato assinado; timestamp de agosto imutável
Esf: M
Risco: LOW
Evidência: release.yml:56-76, pom.xml:71
────────────────────────────────────────
#: 10
Achado: Boot 4.1.0 em 10 lugares, pom em 4.1.1;arada
Cat: docs
Impacto: Badge e snippet de BOM errados
Esf: S
Risco: LOW
Evidência: README.md:8, build-system.md:41

Correção do engine

#: 11
Achado: Fence de conclusão é (node_id, epoch); após Watchdog Bound o mesmo nó re-claima com o mesmo par, e a conclusão zumbi
passa o fence. Hoje só a PK de mohs_attempt imptch inteiro com log enganoso
Cat: bug
Impacto: Fence cuja segurança depende de uma PK que ele desconhece. Confiança MED
Esf: M
Risco: MED
Evidência: PostgresJdbcDelegate.java:193, Enginava:470
────────────────────────────────────────
#: 12
Achado: Enqueued está no sealed e no @OnExecution, mas nenhum site o publica
Cat: bug
Impacto: Listener que compila, valida e nunca r
Esf: S
Risco: decisão
Evidência: ScheduleCommandImpl.java:133, 12 sit
────────────────────────────────────────
#: 13
Achado: Cron SUN-SUN / 7-7 expande para todos os dias
Cat: bug
Impacto: Job semanal roda toda noite, sem aviso
Esf: S
Risco: LOW
Evidência: BitsCronField.java:162
────────────────────────────────────────
#: 14
Achado: isAlive usa lease-ttl no fallback de lie-lease-ttl e explica por quê
Cat: bug
Impacto: Reaper reclama trabalho de nó vivo em cluster misto
Esf: S
Risco: LOW
Evidência: Engine.java:1155 vs :565
────────────────────────────────────────
#: 15
Achado: failSignalAware sem default; o sibling tem
Cat: bug
Impacto: Quarta razão de cancelamento deixa execução sem conclusão
Esf: S
Risco: LOW
Evidência: Dispatcher.java:232
────────────────────────────────────────
#: 16
Achado: Uma linha com priority fora do enum aborta o batch claimado e o tick, em loop
Cat: bug
Impacto: Node claim-crash-reclaim
Esf: S
Risco: LOW
Evidência: Engine.java:1593,1676
────────────────────────────────────────
#: 17
Achado: Flusher que morre por Error deixa inTransit marcado para sempre
Cat: bug
Impacto: Execuções RUNNING invisíveis ao reconcile
Esf: S
Risco: LOW
Evidência: CompletionBatcher.java:191-203
────────────────────────────────────────
#: 18
Achado: StoreTransactions documenta REQUIRED; a implementação é NESTED, e a idempotência depende disso
Cat: docs
Impacto: Próximo "simplificar" quebra idempotên
Esf: S
Risco: LOW
Evidência: StoreTransactions.java:24, JdbcStore
────────────────────────────────────────
#: 19
Achado: Acima de 1000 jobs inadmissíveis, o filqueue volta com visibleAt=now
Cat: bug
Impacto: Livelock de claim consome o budget dos admissíveis. Confiança MED
Esf: M
Risco: MED
Evidência: Engine.java:1363,1447
────────────────────────────────────────
#: 20
Achado: Started é publicado antes de checar handler e cancelamento pré-início
Cat: bug
Impacto: Evento de início para trabalho que nunca rodou
Esf: S
Risco: decisão
Evidência: Dispatcher.java:149-163

Persistência e limites de entrada

#: 21
Achado: Nenhuma statement do tick tem queryTimeout além de 4; sem validação de connectionTimeout do Hikari contra
node-lease-ttl
Cat: bug
Impacto: Tick bloqueado 30 s no pool perde o lease de nó vivo, peers reclamam tudo
Esf: M
Risco: MED
Evidência: JdbcNodeStore.java:47, JdbcLeaseStor:104
────────────────────────────────────────
#: 22
Achado: JobKey e Idempotency-Key sem teto de 25ca em silêncio e colapsa chaves distintas
Cat: bug
Impacto: Job perdido que parece replay idempotente; nos outros dialetos, 500
Esf: S
Risco: LOW
Evidência: JobKey.java:30, JobsController.java:100, schema-*.sql
────────────────────────────────────────
#: 23
Achado: 5 tabelas MySQL da fase 5 sem utf8mb4 eróprio arquivo
Cat: bug
Impacto: Illegal mix of collations ou coerção q
Esf: S
Risco: MED
Evidência: schema-mysql.sql:97-150, V3__table_s
────────────────────────────────────────
#: 24
Achado: JdbcJobStore e JdbcTriggerFirer setam ientro de transação do host o nível é ignorado
Cat: bug
Impacto: Mohs.remove em MySQL roda em REPEATABLE READ
Esf: S
Risco: decisão
Evidência: JdbcJobStore.java:89 vs JdbcWorkQueue.java:73
────────────────────────────────────────
#: 25
Achado: DatabaseClock aceita amostra com round trip qualquer e o clamp monotônico a torna permanente
Cat: bug
Impacto: Nó adiantado reaps leases vivos
Esf: S
Risco: LOW
Evidência: DatabaseClock.java:155-202
────────────────────────────────────────
#: 26
Achado: pruneEmptyBatchesBefore no Postgres/H2 ling tem
Cat: bug
Impacto: Confiança MED, janela estreita
Esf: S
Risco: LOW
Evidência: PostgresJdbcDelegate.java:342
────────────────────────────────────────
#: 27
Achado: Guarda installer-vs-chain só no Postgres; 22 deltas sem verificação, inclusive V8 do SQL Server
Cat: tests
Impacto: Upgrade e install fresco divergem em silêncio
Esf: M
Risco: LOW
Evidência: SchemaPostgresChainMatchesInstallerTest.java:48

REST e segurança

#: 28
Achado: 4 POSTs sem body são requests simples; a doc recomenda csrf.ignoringRequestMatchers
Cat: security
Impacto: Form cross-origin pausa jobs e cancela execuções com sessão do operador
Esf: S
Risco: LOW
Evidência: JobsController.java:121,127, ExecutionsController.java:93,111, security-overview.md:93
────────────────────────────────────────
#: 29
Achado: Payload REST convertido pelo ObjectMapper do host; o store usa mapper próprio pelo motivo oposto
Cat: security
Impacto: Default typing do host vaza para endpoint; campo com typo é descartado em vez de 422
Esf: S
Risco: MED
Evidência: MohsRestAutoConfiguration.java:104, JobsController.java:206
────────────────────────────────────────
#: 30
Achado: cancel e retry não resolvem ator; a dasor
Cat: security
Impacto: Audit trail declarado "Present" não existe para as duas ações mais arriscadas
Esf: M
Risco: LOW
Evidência: ExecutionsController.java:58, api.ts:67-85
────────────────────────────────────────
#: 31
Achado: Teto de assinantes SSE é check-then-actsam todas
Cat: security
Impacto: 5×N leituras no pool do claim, sem aut
Esf: S
Risco: LOW
Evidência: OverviewStreamBroadcaster.java:158,178
────────────────────────────────────────
#: 32
Achado: Id em branco vira 500 com stack trace; ?jobKey= idem
Cat: bug
Impacto: Amplificação de log não autenticada
Esf: S
Risco: LOW
Evidência: ExecutionsController.java:72, sem hation
────────────────────────────────────────
#: 33
Achado: Colisão de nome de rate limit propertiefalham
Cat: bug
Impacto: Limite errado contra recurso externo
Esf: S
Risco: LOW
Evidência: MohsRateLimits.java:65 vs MohsRunners.java:93
────────────────────────────────────────
#: 34
Achado: mohs.api.base-path sem validação; vazio monta a API na raiz e fora do securityMatcher
Cat: bug
Impacto: Location malformado no 202
Esf: S
Risco: LOW
Evidência: MohsProperties.java:153, ExecutionLocations.java:48
────────────────────────────────────────
#: 35
Achado: Mensagens de erro refletem string do caller e internals do Jackson; Attempt.error sem teto
Cat: security
Impacto: Nome de classe do host em 422
Esf: M
Risco: MED
Evidência: RestExceptionHandler.java:70,137, JobsController.java:210
────────────────────────────────────────
#: 36
Achado: Fallback SPA responde 200 text/html para asset inexistente; bundle sem Cache-Control
Cat: bug
Impacto: Dashboard branco após upgrade, com erro MIME. Confiança MED
Esf: S
Risco: LOW
Evidência: MohsUiAutoConfiguration.java:100,146

Performance, com mecanismo lido e sem número

#: 37
Achado: Reconcile de strays lê todos os leases do nó a cada tick e filtra em Java
Cat: perf
Impacto: O(dispatch-concurrency) linhas por tick na thread do heartbeat
Esf: S
Risco: LOW
Evidência: Engine.java:844,1097, PostgresJdbcDelegate.java:412
────────────────────────────────────────
#: 38
Achado: enqueueMembers faz 2 statements por membro apesar das portas aceitarem lista
Cat: perf
Impacto: Batch de 1000 = 2000 round trips numa transação
Esf: S
Risco: LOW
Evidência: MohsImpl.java:201-210
────────────────────────────────────────
#: 39
Achado: Um SELECT de rate limit por definição por lap, três leituras por admissão
Cat: perf
Impacto: O(definições com limite) por tick
Esf: M
Risco: MED
Evidência: Engine.java:1558,1491, JdbcRateLimitStore.java:157
────────────────────────────────────────
#: 40
Achado: Javadoc promete Semaphore; o throttle dusivo, 2 aquisições por dispatch
Cat: docs
Impacto: Regra do CLAUDE.md não vale; contenção
Esf: S doc
Risco: LOW
Evidência: MohsExecutors.java:35-78, Spring 7.0:73
────────────────────────────────────────
#: 41
Achado: pollCancelRequests manda todos os ids i
Cat: perf
Impacto: 1024 parâmetros a 20/s para um resultado quase sempre vazio
Esf: S
Risco: contrato
Evidência: Engine.java:813,1801
────────────────────────────────────────
#: 42
Achado: Scanner segura synchronized durante todos os upserts de boot
Cat: perf
Impacto: Boot paralelo do Spring 6.2+ serializado
Esf: S
Risco: LOW
Evidência: MohsJobScanner.java:218-227
────────────────────────────────────────
#: 43
Achado: close() do broadcaster não espera os emitters
Cat: bug
Impacto: Shutdown queima os 30 s da fase web. Confiança MED
Esf: S
Risco: LOW
Evidência: OverviewStreamBroadcaster.java:471

Testes e ferramental

#: 44
Achado: JaCoCo sem agregação: engine reporta 23 store; nada gateia
Cat: tests
Impacto: O número do módulo mais crítico é inútil
Esf: S
Risco: LOW
Evidência: pom.xml:242, mohs-engine/target/site/jacoco/jacoco.csv
────────────────────────────────────────
#: 45
Achado: Nenhum teste do reator sobe dois Engine; a suíte de cluster existe em *Scenario e nunca roda no CI
Cat: tests
Impacto: Nenhuma prova de interleaving entre nós
Esf: M
Risco: MED
Evidência: EngineTest.java 19 engines isolados,
────────────────────────────────────────
#: 46
Achado: Invariantes de concorrência só no H2: b CAS de trigger sequencial; MySQL sem teste de
SKIP LOCKED
Cat: tests
Impacto: Os três dialetos vendidos não provam o que o H2 prova
Esf: M
Risco: MED
Evidência: JdbcRateLimitStoreTest:271, JdbcBatchStoreTest:126, JdbcTriggerFirerTest:137
────────────────────────────────────────
#: 47
Achado: CI justifica o cache do bundle citando o existe; o resolver SPA anti-traversal não tem
teste
Cat: tests
Impacto: Bundle quebrado passa verde
Esf: S
Risco: LOW
Evidência: maven.yml:35, MohsUiAutoConfigurationTest
────────────────────────────────────────
#: 48
Achado: CompositeCronField 13% coberto; #, L, W com um valor cada
Cat: tests
Impacto: Job dispara no dia errado
Esf: M
Risco: LOW
Evidência: jacoco.csv do cron, CronExpressionTest.java
────────────────────────────────────────
#: 49
Achado: Sem @Tag: ./mvnw verify sem Docker fica vermelho, indistinguível de regressão
Cat: dx
Impacto: Sem comando degradado documentado
Esf: S
Risco: LOW
Evidência: 0 @Tag no reator, maven.yml:88
────────────────────────────────────────
#: 50
Achado: Sem teste: BatchCompletionCallbacks (API pública, LRU), OnExecutionRegistry (7 de 8 rotas), conflação SSE, misfire ×
DST
Cat: tests
Impacto: Silêncio por construção
Esf: S cada
Risco: LOW
Evidência: arquivos citados nos relatórios
────────────────────────────────────────
#: 51
Achado: InMemoryJobStore publicado sem teste de paridade com JdbcJobStore
Cat: tests
Impacto: Falsa confiança no usuário do kit
Esf: M
Risco: MED
Evidência: mohs-test/.../InMemoryJobStore.java

Dívida e DX

#: 52
Achado: Os 4 delegates JDBC têm 92 a 96% de linhas idênticas; a interface já tem o mecanismo default e usa em 2 de 72
Cat: debt
Impacto: Shotgun surgery a cada mudança de SQL
Esf: L
Risco: MED
Evidência: diff H2×MySQL: 52 linhas de 1306
────────────────────────────────────────
#: 53
Achado: ~30 citações DBTUNE-nn em 12 SQL aponta 5 delas são a única justificativa
Cat: debt
Impacto: Operador lê o SQL à mão
Esf: S
Risco: LOW
Evidência: postgresql/V2__node_lease.sql:14-26
────────────────────────────────────────
#: 54
Achado: Validação de config espalhada: completion-flush-on-every-result lido por literal, sync-interval, grace-period,
skew-warn-threshold sem checagem
Cat: dx
Impacto: Erro do Spring sem nome da propriedade
Esf: S
Risco: LOW
Evidência: MohsProperties.java:95,112,123,124
────────────────────────────────────────
#: 55
Achado: module-info do engine requires spring.beans sem uso; spring-core usado sem declarar no pom
Cat: debt
Impacto: Módulo extra no module path
Esf: S
Risco: LOW
Evidência: module-info.java:31,34
────────────────────────────────────────
#: 56
Achado: CodeQL só Java; dashboard sem lint nem
Cat: dx
Impacto: Uma linha de matriz
Esf: S
Risco: LOW
Evidência: codeql.yml:26
────────────────────────────────────────
#: 57
Achado: Miudezas: Node 22 no build vs @types/node 26; 2 primitivos shadcn mortos; .claude/settings.json com comentário
auto-referente; sem .editorconfig
Cat: dx
Impacto: Baixo
Esf: S
Risco: LOW
Evidência: mohs-ui/pom.xml:40, .claude/settings

Sweep de docs menores, juntos num plano só: piso de 12 s do node-lease-ttl ausente e tuning.md convidando a 8 s; notice do
PATCH em português na doc; inventário de testines no demo que diz não ter; 4 citaçõesTipo#membro mortas; label history-prune faltando em mohs.tick.failed; documentation-audit.md "Active" com uma dúzia de números falsos; modules.md com deprecation inexistente e JPMS incompleto; 73 de 76 docs com Last Reviewed anterior ao
próprio commit; security-overview.md dizendo "nSECURITY.md prometendo "drain nodes" que nãoexiste; metrics.md "queue depth não é métrica".

Direção, opções para você pesar

- Drain de nó pelo REST. MohsLifecycle tem drain, o REST só tem GET /nodes, e o WARN de boot promete o contrário. É o gap exato do differentiator "graceful shutdown" citado no CLAUDE.md e o que torna o preStop do Kubernetes uma linha.
  Trade-off: drain remoto exige estado em mohs_n do próprio nó é barato. Esforço M.
- Kit de teste real. mohs-test publica 2 classes; 31 testes de conformidade por dialeto ficam em test scope. Nenhuma forma
  de testar um handler @MohsJob nem de validar ço L, ou M por metade.
- Deltas apontáveis pelo Flyway do host. Os V*.sql já estão em diretório por dialeto; o que impede
  flyway.locations=classpath:... é colisão de venomear pré-1.0 é grátis; a receita precisa deteste. Esforço S a M.
- GET /batches. Batches são capability de capa shboard não consegue listá-los. Esforço S. Se aresposta for "criação só por Java", registrar.

Rejeitados na vetagem

- Guardas de adoção de mohs_nodes nos installers H2/MySQL/Postgres: installer é para banco novo, upgrade é o V2, e bases
  pré-1.0 são descartáveis por decisão.
- Índice parcial em correlation_id, contenção de última página no SQL Server, índice em mohs_ready(job_key), chunk de aliveNodeIds: valem uma medição, não um plano.
- ExecutionQuery.limit sem teto: o REST já clame. Baixo.
- Testes com Thread.sleep (3 sites): o único que importa é o de pool 1 que passa vazio. Baixo.

Não coberto

Nenhum agente executou build ou teste; tudo é leitura. Não vetado: os 30 SQL em português statement por statement,
docs/10-performance contra o benchmark, os 29 ds-cron contra o upstream, versões atuais dasdependências.

Ordem que recomendo para os planos

Minha sugestão de 10 planos, com dependências:

1. Parar de publicar o test-jar e endurecer o r
2. CLAUDE.md e CHANGELOG verdadeiros (6, 7, 8).
3. Sweep de docs: features "ausentes", ArchUnit, groupId, migrate, versão do Boot, e os menores (2, 3, 4, 5, 10, sweep).
4. Timeouts em toda statement do tick e validaçhasVisibleWork).
5. Fence com attempt_number (11), com o teste de re-claim no mesmo nó.
6. Lote de correções pequenas do engine (13, 14, 15, 16, 17, 18, 26).
7. Limites na fronteira: comprimento de chaves,lisão de rate limit, charset MySQL (22, 23, 32,33, 34).
8. REST: CSRF, SSE atômico, mapper próprio, ator em cancel/retry (28, 29, 30, 31).
9. JaCoCo agregado, @Tag("docker"), testes que  49, 50, 47).
10. Dois nós de verdade e invariantes nos três dialetos (45, 46, 27). Depende do 9.

Ficam para decisão sua antes de virar plano: Eno sealed), Started antes do handler,REQUIRES_NEW em Mohs.remove, e as quatro opções de direção.

Quais viram plano? Se disser "os 10", escrevo os dez nessa ordem em plans/012 a 021.



Madrugada encerrada com tudo verificado e registrado. Resumo para a manhã:

Resultado: seis lotes de legibilidade, todos com comportamento e API pública preservados, cada um com refactorer, db-tuner (quando tocou persistência), revisor ✅ e reator completo verde. Último reator (run-6) com Docker, 846 testes, zero falhas. As duas últimas edições foram só de Javadoc no Engine, verificadas por compilação. Nada foi commitado; a árvore tem cerca de 165 arquivos modificados desde 7906e4c.

O que ficou mais simples:
- mohs-rest: helper problem() no handler, JobsController.enqueue com guard clauses, convertPayload em forma explícita, respostas de batch com from, prosa em inglês nos package-info.
- Starter e API: scanMethod dividido em observer e job, validação do MohsRunner por modo, ExecutionWindow.inUtc como único ponto do "avaliado em UTC".
- Stores JDBC: JdbcJobStore.remove lê em quatro passos nomeados, insertOrRedecide, readSchedule, deriveState no histórico, helpers …OrNull de timestamp usados também pelos delegates.
- Engine: Dispatcher.terminalResult no lugar de três construções de 14 argumentos, ScheduleCommandImpl.at em enqueue/onDurable/receiptOfTheWinner, e três extrações conservadoras no Engine.java sem tocar em locks ou ordem do tick.

Decisões que ficaram para você (mudam comportamento ou API):
- Teste intermitente pré-existente: o jitter do backoff pode ser 0 ms, e aí o estado derivado vira ENQUEUED em vez de RETRY_WAITING. Três asserções em DispatcherTest estão expostas. Opções: jitter mínimo de 1 ms no engine, ou fixar o random nos testes.
- Limitar a profundidade do walk de causas em RnCause.
- Construtor de 16 parâmetros em MohsImpl, split de Engine.java e de EngineTest, e um withSchedule em JobDefinition para o kit de teste.

Tudo isso está detalhado no arquivo de estado da sessão na memória, seção "Relatório para a manhã".
