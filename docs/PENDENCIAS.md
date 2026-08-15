# Pendências — decisões em aberto

Origens: `codereview-20260815-0332.md` (segunda passada, "Perguntas ao
autor"), o "Fora do escopo" do plano de refactor de `io.mohs.autoconfigure`
(executado e removido em 709d5b2) e achados registrados ao resolver itens
anteriores. Todas as correções do review foram aplicadas; estes itens são
as decisões que ficaram com o autor. Ao resolver um item, registrar a
decisão (ADR ou Javadoc, conforme o caso) e removê-lo daqui — a numeração
dos demais não muda.

10. **Evolução de schema antes do primeiro release** — os quatro
    `schema-*.sql` usam `CREATE TABLE IF NOT EXISTS`: base criada antes de
    uma coluna nova (ex.: `cancel_requested`, ADR-0034) não a ganha e quebra
    no boot. Decisão de 2026-08-15: pré-GA vale drop-and-recreate; formalizar
    expand/contract (ADR próprio) antes do primeiro usuário externo.
