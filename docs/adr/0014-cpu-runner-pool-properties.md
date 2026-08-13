# ADR-0014: Properties de pool estilo Spring para o runner CPU

## Status
Decided — 2026-08-13

## Context
`MohsRunner` (ADR 0004, §5.7 do documento mestre) nasceu em M1 com um
único campo configurável, `maxConcurrent`, usado da mesma forma para os
dois modos (`IO`/`CPU`). Isso é suficiente pro modo `IO` (o único ajuste
que faz sentido pra virtual threads é o teto do `Semaphore`), mas é pobre
demais pro modo `CPU`, onde existe um pool de platform threads de verdade
por trás — exatamente o tipo de recurso que o Spring Boot expõe em
`spring.task.execution.pool.*`
(`core-size`/`max-size`/`queue-capacity`/`keep-alive`, classe
`TaskExecutionProperties.Pool`) e, desde a 3.2, complementa com
`spring.threads.virtual.enabled` pra trocar o executor inteiro por um
baseado em virtual threads sem pool algum.

## Decision
1. **`MohsRunner` continua um record único e flat**, não vira sealed
   `IoRunner`/`CpuRunner`. Alternativa considerada e rejeitada: sealed
   hierarchy (mesmo padrão de `Schedule`/`CronSpec`/`IntervalSpec`), que
   tornaria estados sem sentido irrepresentáveis em compilação (ex.: `CPU`
   com `maxConcurrent` preenchido). Rejeitada porque, ao contrário de
   `Schedule` — onde `Execution`/o motor precisam fazer `switch` exaustivo
   sobre a variante —, nada no motor hoje ou no design documentado
   precisa de despacho polimórfico sobre "tipo de runner"; a única
   consumidora do formato é a fiação que cria o `Executor` real (ainda não
   construída), que já vai fazer `switch (mode)` de qualquer forma. Sealed
   compraria segurança de tipo que ninguém ia gastar, ao custo de duas
   classes de builder que já bastam pra mesma garantia na prática.
2. **`maxConcurrent`** vale só para `RunnerMode.IO`; **`coreSize`**,
   **`maxSize`**, **`queueCapacity`**, **`keepAlive`** valem só para
   `RunnerMode.CPU` — o grupo do modo errado fica zerado (`0`/
   `Duration.ZERO`) e é ignorado pela validação do outro ramo.
   `IoBuilder`/`CpuBuilder` (dois builders, não um) impedem o erro de uso
   em tempo de compilação sem precisar do tipo armazenado ser selado.
3. **Defaults do modo `CPU` divergem dos defaults do Spring,
   deliberadamente:**
   - `coreSize` e `maxSize` default para `Runtime.getRuntime().availableProcessors()`
     — pool fixo (sem elasticidade) por padrão. O Spring deixa `maxSize`
     em `Integer.MAX_VALUE`: faz sentido lá, onde o executor é genérico e
     não sabe se o trabalho é CPU ou I/O-bound; aqui sabemos que é
     CPU-bound, e mais threads que núcleos não ajuda esse tipo de
     trabalho — a "disciplina io→virtual/cpu→platform" (CLAUDE.md,
     ADR-0004) já decidiu isso.
   - `queueCapacity` default para `0` (direct handoff, estilo
     `SynchronousQueue`): task só entra se houver thread livre ou espaço
     até `maxSize`; do contrário, rejeita imediatamente. O Spring deixa
     `Integer.MAX_VALUE` (fila ilimitada). Ilimitada aqui violaria a regra
     já registrada no `CLAUDE.md`: "Backpressure e limites em toda
     borda... nunca OOM ou espera infinita". Quem precisa absorver rajada
     usa `JobQueue` — o eixo cluster-wide certo pra isso (papéis que nunca
     se fundem, §5.8 do documento mestre) — não uma fila escondida dentro
     do runner.
   - `keepAlive` default para 60s, igual ao Spring, mas só produz efeito
     observável quando alguém configura `maxSize > coreSize`
     explicitamente — semântica padrão de `ThreadPoolExecutor` (keep-alive
     não se aplica a core threads por default). Com `coreSize == maxSize`
     por padrão aqui, isso é inócuo até o dia em que alguém pedir
     elasticidade de verdade.
4. **`allowCoreThreadTimeout` (que o Spring tem) ficou de fora.** Sem
   elasticidade por padrão, essa opção não tem uso real ainda — YAGNI.
   Fica pra quando `maxSize > coreSize` virar caso comum o suficiente pra
   justificar.

## Consequences
`MohsRunner.CPU` fica mais verboso que o `MohsRunner.IO` (quatro campos
contra um), mas cada campo tem semântica clara e testada
(`MohsRunnerTest`). A divergência de defaults em relação ao Spring é
deliberada e documentada aqui — quem migra intuição do
`spring.task.execution.pool.*` direto pro Mohs vai notar que
`queueCapacity`/`maxSize` não são "ilimitados por padrão" e precisa ler
esta ADR (ou o Javadoc de `MohsRunner`, que aponta pra cá) pra entender
por quê. A fábrica real de `Executor`/`ExecutorService` a partir destes
campos ainda não existe — é motor (M3), fica para quando for pedida.

## Source
Conversa de revisão de `RunnerMode`/`MohsRunner` (2026-08-13);
`org.springframework.boot.task.TaskExecutionProperties.Pool` e
`spring.threads.virtual.enabled` (Spring Boot) como referência de forma,
não de defaults; `docs/MOHS-DOCUMENTO-MESTRE.md` §5.7 e §3 ("Runner
node-local... virtual cap 64 / cpu = cores"); `docs/adr/0004-vocabulary-and-renames.md`.
