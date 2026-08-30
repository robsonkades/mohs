# ADR-0011: Serialização e versionamento de payload

## Status
Decided — 2026-08-12

## Context
Payloads de job são serializados para persistência e desserializados na
execução; entre o momento em que um job é agendado e o momento em que ele
executa (ou entre deploys, quando um Execution antigo ainda está
pendente), o formato do payload no código do handler pode ter mudado. É
preciso decidir de quem é a responsabilidade de garantir compatibilidade
nesse intervalo.

## Decision
Compatibilidade de payload entre deploys é **obrigação do
handler/aplicação, não do motor**. O Mohs garante apenas o round-trip de
serialização no boot (validação de boot 4 — payload serializável, testado
por round-trip) — não garante, nem tenta migrar, Executions já
persistidas contra um handler cujo payload mudou de forma. Quebra de
contrato do lado da aplicação não tem rede de segurança do motor.

## Consequences
O motor permanece simples — não carrega lógica de migração de schema de
payload nem versionamento embutido. O custo desse limite recai
inteiramente sobre a aplicação: se um payload muda de forma de modo
incompatível entre um `schedule()` e a execução real (ex.: deploy no meio
do caminho), a desserialização pode falhar em runtime sem qualquer
proteção do Mohs — cabe ao handler/aplicação desenhar seus payloads para
evolução compatível (campos opcionais, versionamento próprio) se esse
cenário for uma preocupação real.

## Source
docs/MOHS-DOCUMENTO-MESTRE.md §7 "Resolvidas" item 2 (lines 528-530);
docs/API-DESIGN.md "Decidido (12/08/2026)" primeiro bullet (lines
621-626)
