# ADR-0053 — Cap de concorrência derivado da posse

Data: 2026-08-22 · Status: aceita · Fase: Phase 5 do redesign (ADR-D do plano; substitui o mecanismo das ADR-0018/0020, aposenta a ADR-0025)

## Contexto

O mutex/teto por job (`maxConcurrentExecutions`) era um contador
persistido — `mohs_job_definitions.running_execution_count` — mantido por
CAS guardado dentro da transação de claim (ADR-0018/0020) e decrementado
na conclusão. Três custos: uma hot row por job com cap DENTRO do caminho
mais quente do sistema; vazamento estrutural (crash entre incremento e
conclusão deixa a vaga presa — a ADR-0025 existia só para o reaper
devolvê-la); e o acoplamento do flusher da ADR-0047 (a devolução de vagas
serializada foi o primeiro gargalo medido da Phase 3).

Com o split (ADR-0052), a posse virou linha própria: `mohs_lease` contém
EXATAMENTE o conjunto "executando agora", por construção.

## Decisão

1. **O contador morreu.** `running_execution_count` saiu do schema
   (migração V4), `tryIncrementRunningExecutions`/`decrementRunningExecutions`
   saíram da porta `JobStore`, e a ADR-0025 morre por construção: deletar
   a lease É liberar a vaga — não existe mais recurso para o reaper
   devolver.
2. **A folga deriva da posse**: uma `COUNT(*) GROUP BY job_key` sobre
   `mohs_lease` (`LeaseStore.countByJob`), lida UMA vez por rodada de
   claim, só para os jobs com cap (`Admission.compute`, §5.4). Job no
   teto entra na lista de inadmissíveis da rodada; sobra de admissão
   pós-claim volta pra fila pelo requeue cercado, com o MESMO attempt
   (perda de admissão nunca consome orçamento) e conta em
   `mohs.claim.requeued{reason="concurrency-cap"}`.
3. **Cap é SOFT por contrato**: sobre-admissão limitada a 1 rodada × nós,
   corrigida na rodada seguinte (a contagem é um snapshot da rodada). Um
   job recém-nascido fora do snapshot do tick passa pelos MESMOS guards
   via consulta fresca memoizada (`storedJobFor` — review S5.4, que pegou
   o cap cego nas rodadas 2+ sem a memoização).

## Alternativas consideradas

- **Manter o contador** — mantém a hot row, o vazamento e a ADR-0025.
- **Tabela de slots** — dá cap HARD; guardada como escape hatch
  documentado para quando um job exigir dureza (drop-in: a posse não
  muda).

## Consequências

- Zero escrita de contador no claim e na conclusão; o custo virou uma
  leitura por rodada sobre uma tabela que vive pequena (`countByJob`
  medido em 0,048ms/7 buffers com 1k leases — tuning do S5.3).
- Quem contratou mutex (`allowConcurrentExecutions=false`) tolera
  sobre-admissão transitória de até 1 rodada × nós — para estado
  compartilhado que não tolera NENHUMA sobreposição, o escape hatch é a
  resposta, não este mecanismo (documentado no Javadoc do Engine).
- `GET /jobs` perdeu o contador pronto; a contagem viva vem de
  `countByJob` quando o endpoint precisar (o `JobSnapshot` público nunca
  o expôs).
- Reversibilidade alta: a tabela de slots é drop-in; o contador poderia
  voltar por migração — nada além do `JobStore` o conhecia.
