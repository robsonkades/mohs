# ADR-0015: Consolidar a API pública sob `io.mohs.core`

## Status
Decided — 2026-08-13

## Context
A ADR-0013 dividiu o vocabulário público em subpacotes coesos
(`io.mohs.schedule`, `.definition`, `.execution`, `.event`, `.resource`),
mas manteve a fachada e a identidade compartilhada (`Mohs`, `JobKey`,
`ExecutionId`, `JobRef`, `MohsLifecycle`, `ScheduleCommand`, `Batch`,
`BatchBuilder`, `EngineState`) soltas em `io.mohs` (raiz). Isso deixava a
fronteira entre "API pública" e "bootstrap deste módulo"
(`MohsApplication`) implícita — ambas compartilhavam o mesmo pacote.

`docs/MOHS-DOCUMENTO-MESTRE.md` já nomeia o M1 inteiro como "Contratos
**do core**" (`io.mohs`). Faltava um pacote que desse nome literal a esse
conceito e separasse explicitamente API/domínio de infraestrutura.

## Decision
Tudo que hoje é API pública migra para `io.mohs.core`: a fachada e
identidade que estavam na raiz, mais os cinco subpacotes de domínio da
ADR-0013 (agora `io.mohs.core.schedule`, `.definition`, `.execution`,
`.event`, `.resource`). `io.mohs` (raiz) fica só com
`MohsApplication` — o bootstrap Spring Boot deste módulo, não API da
biblioteca.

**Duas exclusões deliberadas:**

1. **`io.mohs.cron` não migra.** Não é vocabulário de job — não conhece
   `JobKey`/`Schedule`/`JobDefinition`, é maquinário que o motor consome
   (`NextFireCalculator`, M3). "Core" aqui é o vocabulário de agendamento
   de M1, não qualquer coisa que não seja `engine`/`jdbc`/etc.
2. **Nenhuma regra ArchUnit nova.** A regra de fronteira da ADR-0013
   (`io.mohs.. menos os 5 pacotes internos = API pública`) cobre
   `io.mohs.core.*` automaticamente — a checagem por exclusão dos
   internos (em vez de inclusão dos públicos) foi desenhada exatamente
   para absorver reorganizações como esta sem edição. Rodou sem tocar no
   arquivo: confirma a aposta da ADR-0013 na prática, não só na teoria.

Isto **revisa a ADR-0013** (que descrevia a fachada/identidade como parte
do `io.mohs` raiz plano). A ADR-0013 permanece como registro histórico,
com nota apontando pra esta.

## Consequences
FQN de praticamente todo tipo público muda de novo — desta vez incluindo
a fachada (`io.mohs.Mohs` → `io.mohs.core.Mohs`), uma mudança maior que a
da ADR-0013. Sem consumidor externo ainda dependendo do FQN antigo, o
custo é só de sincronizar os próprios docs deste repositório.

Ganho: `io.mohs` (raiz) deixa de ser ambíguo — hoje é só bootstrap deste
módulo (`MohsApplication`), nunca API. Quem for ler o código sabe que
"API pública do Mohs" tem exatamente um endereço: `io.mohs.core` e seus
subpacotes.

## Source
Conversa de criação de `io.mohs.core` (2026-08-13); revisa
`docs/adr/0013-public-api-subpackaging.md`;
`docs/MOHS-DOCUMENTO-MESTRE.md` §9 (nomeia M1 como "Contratos do core");
`src/test/java/io/mohs/ArchitectureTest.java` (não editado — evidência de
que a regra da ADR-0013 já cobria isto).
