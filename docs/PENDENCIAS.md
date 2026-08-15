# Pendências — decisões em aberto

Origem: itens 1–4 de `codereview-20260815-0332.md` (segunda passada,
"Perguntas ao autor"); itens 5–6 do "Fora do escopo" do plano de refactor
de `io.mohs.autoconfigure` (executado e removido em 709d5b2). Todas as
correções do review foram aplicadas; estes itens são as decisões que
ficaram com o autor. Ao resolver um item, registrar a decisão (ADR ou
Javadoc, conforme o caso) e removê-lo daqui.

## 1. Janela de ~24h da Idempotency-Key

O dedupe por `(job_key, idempotency_key)` está implementado e testado nos 4
dialetos (índice único `uq_mohs_executions_idem` + Idempotent Receiver em
`ScheduleCommandImpl`), mas a chave hoje vale **para sempre** — documentado
no Javadoc de `ScheduleCommand.idempotencyKey`. O design
(`REST-API-DESIGN.md`) pede janela de ~24h.

**Decidir:** o TTL/purga entra ainda no M3, ou vira ADR própria junto com a
política de retenção de execuções (as duas purgas compartilham mecanismo)?

## 3. `mohs.api.base-path` sem leitor

A propriedade existe em `MohsProperties.Api`, mas nenhum código de produção
a lê — controllers e o header `Location` usam o placeholder
`${mohs.api.base-path:...}` direto. Funciona (o binder valida e o metadata
dá autocomplete), mas a fonte é duplicada: o default vive em `ApiPaths.V1`
e em `MohsProperties.Api.basePath`.

**Decidir:** manter a classe só como metadata/`@ConditionalOnProperty`
(documentar isso nela), ou unificar a fonte (controllers passam a receber o
valor via `MohsProperties`).

## 4. `@OnExecution` — milestone do processamento

A anotação existe na API pública mas o motor não a processa. Correção
provisória aplicada (N1): `MohsJobScanner` **falha o boot** ao encontrá-la,
com mensagem apontando a alternativa (`ExecutionListener` como bean), e o
Javadoc da anotação declara o status.

**Decidir:** em qual milestone entra a entrega de eventos filtrados a
métodos anotados. Quando entrar, o guard do fail-fast em
`MohsJobScanner.scanMethod` é o ponto exato a substituir pelo registro do
listener sintetizado.

## 5. Nenhum bean interno recua com `@ConditionalOnMissingBean`

Todos os beans de `MohsAutoConfiguration` são incondicionais — o
consumidor não consegue substituir `Clock`, stores nem executores (o
`JsonMapper` de persistência já está decidido: ADR-0029). Pode ser
deliberado (internos não são SPI), mas é uma decisão de superfície de API
que merece registro, não um estado implícito.

**Decidir:** confirmar que os internos não são pontos de extensão e
registrar em mini-ADR — ou escolher quais beans viram SPI substituível e
só então dar `@ConditionalOnMissingBean` a esses.

## 6. Payload de tipo errado em `MohsJobs.adaptHandler`

Estoura como `IllegalArgumentException` crua do reflection ("argument
type mismatch"), sem dizer job nem método. Melhorar a mensagem muda o
conteúdo de `Attempt.error()` — comportamento observável —, então
depende de aprovação explícita, não entra como refactor.

**Decidir:** aprovar (ou não) a mensagem contextualizada. Quando
aprovada, o ponto exato é a invocação em `MohsJobs.adaptHandler`,
nomeando job e método declarante no erro.
