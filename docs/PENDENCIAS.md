# Pendências — decisões em aberto

Origens: `codereview-20260815-0332.md` (segunda passada, "Perguntas ao
autor"), o "Fora do escopo" do plano de refactor de `io.mohs.autoconfigure`
(executado e removido em 709d5b2) e achados registrados ao resolver itens
anteriores. Todas as correções do review foram aplicadas; estes itens são
as decisões que ficaram com o autor. Ao resolver um item, registrar a
decisão (ADR ou Javadoc, conforme o caso) e removê-lo daqui — a numeração
dos demais não muda.

## 8. Desfechos do reaper não publicam eventos

Apontado pelo review do retry (ADR-0033). O caminho do dispatcher publica
`AttemptFailed`/`RetryScheduled`/`Failed`, mas os desfechos decididos pelo
reaper — exatamente as falhas por morte de nó, o gancho de alerta que o
Javadoc de `Failed` anuncia — não emitem nada. A assimetria está declarada
no Javadoc de `ExecutionListener` como limitação da rodada; o retorno de
`reclaimExpired()` já carrega estado e attempt por execução, então o
`Engine.tick` tem o material pra publicar.

**Decidir:** expor o publisher ao `Engine` (hoje interno ao `Dispatcher`)
e publicar os eventos do reclaim — ou manter a assimetria documentada como
contrato. Encaixa naturalmente no trabalho do watchdog (mesmo miolo).

## 9. SELECT de candidatos do reaper sem teto

Apontado pelo review do retry. `JdbcReaper.selectExpiredCandidates` não
tem `LIMIT` — morte de nó com milhares de execuções em voo produz um lote
de reclaim sem teto na mesma transação (a regra do projeto pede
comportamento definido em toda borda). Encaixa no trabalho do watchdog.

**Decidir:** teto por ciclo (drena em várias passadas) e seu valor.
