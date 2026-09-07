# Volume de execucoes pela API

O script [api-load.ps1](../../mohs-benchmark/scripts/api-load.ps1) gera execucoes de
**jobs ja registrados** via `POST /jobs/{jobKey}/schedule`. Ele nao cria definicoes nem
modifica cron ou intervalos. A API atual nao oferece cadastro de novas definicoes.
Jobs recorrentes tambem podem receber essas invocacoes manuais; sua recorrencia continua ativa.

Use PowerShell 7 e uma aplicacao com `mohs.api.enabled=true`. `BaseUrl` deve incluir
o context path e o caminho da API, se personalizados. O script valida cada job com GET
antes de enviar carga. Ele nao inicia a aplicacao nem altera o banco diretamente.

```powershell
# Simular os parametros, sem chamadas HTTP ou arquivos.
./mohs-benchmark/scripts/api-load.ps1 -JobKeys every-job2 -Count 10000 -DryRun

# Enfileirar 10 mil execucoes, em ondas de 500, com ate 16 requisicoes simultaneas.
./mohs-benchmark/scripts/api-load.ps1 `
    -BaseUrl http://localhost:8080/api/mohs/v1 `
    -JobKeys every-job2 -Count 10000 -WaveSize 500 -Concurrency 16 -WaveIntervalSeconds 1

# Distribuir 2 mil execucoes no total entre dois jobs existentes.
./mohs-benchmark/scripts/api-load.ps1 `
    -JobKeys every-job,every-job2 -Count 2000 -WaveSize 100 -Concurrency 8

# Um payload comum a todos os jobs selecionados; deve ser compativel com seus handlers.
./mohs-benchmark/scripts/api-load.ps1 `
    -JobKeys send-invoice -PayloadJson '{"invoiceId":4711}' -Count 1000
```

`Count` e o total, nao a quantidade por job ou por onda. A distribuicao segue a ordem
de `JobKeys`. `WaveIntervalSeconds` e a pausa **apos** concluir uma onda; a duracao
das requisicoes tambem conta, portanto o script nao garante uma taxa fixa de chegada.
`Concurrency` limita requisicoes HTTP em paralelo, nao a concorrencia dos handlers.
O script termina depois da ultima resposta HTTP, sem aguardar os jobs terminarem.

Se o host exigir autenticacao, configure `MOHS_API_TOKEN` no ambiente para Bearer
ou passe `-Headers` com os cabecalhos exigidos, inclusive CSRF quando aplicavel.
`-Actor` tem padrao `api-volume-test`; `-Priority`, `NORMAL`; `-TimeoutSeconds`, 30.
O payload padrao e `{}`, adequado aos handlers sem payload da demo.

Cada chamada usa uma chave de idempotencia unica por `RunId` e ordinal. Nao ha retry
automatico. A primeira onda com erro interrompe as ondas seguintes, preservando os
resultados no CSV. Timeout, erro de transporte ou HTTP 5xx podem ocorrer apos o commit:
confirme o resultado antes de repetir. Reutilizar `RunId` so reproduz as mesmas chaves
se ordem dos jobs, quantidade e payload forem mantidos, dentro da retencao de idempotencia.
Use outro `OutputPath` ao reproduzir uma tentativa: um CSV existente nunca e sobrescrito.

O CSV padrao fica em `mohs-benchmark/results/api-load-<RunId>.csv`, com job, chave de
idempotencia, executionId, status HTTP, resultado e latencia de cada requisicao. O resumo
mede **aceites da API**, que podem incluir respostas deduplicadas, nao conclusoes dos jobs.
Acompanhe fila, execucoes e throughput pelo dashboard ou por `GET /overview`.

O handler `every-job2` da demo escreve um log por execucao. Esse custo faz parte da carga;
resultados com ele nao representam o custo isolado do scheduler.
