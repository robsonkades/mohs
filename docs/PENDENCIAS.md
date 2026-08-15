# Pendências — decisões em aberto

Origens: `codereview-20260815-0332.md` (segunda passada, "Perguntas ao
autor"), o "Fora do escopo" do plano de refactor de `io.mohs.autoconfigure`
(executado e removido em 709d5b2) e achados registrados ao resolver itens
anteriores. Todas as correções do review foram aplicadas; estes itens são
as decisões que ficaram com o autor. Ao resolver um item, registrar a
decisão (ADR ou Javadoc, conforme o caso) e removê-lo daqui — a numeração
dos demais não muda.

## 1. Política de retenção de execuções

Linhas terminais de `mohs_executions` nunca saem, por desenho — apontado
pelo review de tuning (DBTUNE-9) como decisão de produto sem ADR. A
ADR-0030 amarrou a janela da Idempotency-Key a esta política (a chave
deduplica enquanto a execução existir), o que acrescenta um requisito: a
janela de retenção não pode ficar abaixo de ~24h (mínimo prometido pelo
design REST).

**Decidir:** a política em si — delete vs. arquivamento, janela default,
configuração, job interno do Mohs como mecanismo (candidato do DBTUNE-9).
Vira ADR própria quando desenhada.

## 4. `@OnExecution` — milestone do processamento

A anotação existe na API pública mas o motor não a processa. Correção
provisória aplicada (N1): `MohsJobScanner` **falha o boot** ao encontrá-la,
com mensagem apontando a alternativa (`ExecutionListener` como bean), e o
Javadoc da anotação declara o status.

**Decidir:** em qual milestone entra a entrega de eventos filtrados a
métodos anotados. Quando entrar, o guard do fail-fast em
`MohsJobScanner.scanMethod` é o ponto exato a substituir pelo registro do
listener sintetizado.
