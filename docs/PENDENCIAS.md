# Pendências — decisões em aberto

Origem: `codereview-20260815-0332.md` (segunda passada, "Perguntas ao autor").
Todas as correções do review foram aplicadas; estes 4 itens são as decisões
que ficaram com o autor. Ao resolver um item, registrar a decisão (ADR ou
Javadoc, conforme o caso) e removê-lo daqui.

## 1. Janela de ~24h da Idempotency-Key

O dedupe por `(job_key, idempotency_key)` está implementado e testado nos 4
dialetos (índice único `uq_mohs_executions_idem` + Idempotent Receiver em
`ScheduleCommandImpl`), mas a chave hoje vale **para sempre** — documentado
no Javadoc de `ScheduleCommand.idempotencyKey`. O design
(`REST-API-DESIGN.md`) pede janela de ~24h.

**Decidir:** o TTL/purga entra ainda no M3, ou vira ADR própria junto com a
política de retenção de execuções (as duas purgas compartilham mecanismo)?

## 2. Payload persistido com `JsonMapper` cru

`MohsAutoConfiguration.mohsExecutionStore` serializa o payload com
`JsonMapper.builder().build()`, deliberadamente(?) isolado do
`ObjectMapper` customizado do host — enquanto a REST converte o corpo da
request com o mapper do contexto. Se o isolamento é decisão de estabilidade
do formato persistido (módulos/configurações do host não podem mudar como
payloads antigos foram gravados), ela está invisível: é fácil alguém
"consertar" para o mapper do contexto e quebrar a leitura de payloads já
persistidos.

**Decidir:** confirmar a intenção e registrá-la (linha de Javadoc no bean ou
mini-ADR) — ou, se não for intencional, trocar pro mapper do host **antes**
do primeiro payload persistido em produção, enquanto não há dado antigo pra
quebrar.

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
