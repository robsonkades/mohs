# ADR-0045: Dashboard em `mohs-ui`, consumindo a API REST pública

## Status
Decided — 2026-08-21

## Context
O Mohs tem uma API REST operacional desenhada e implementada (ADR-0010,
`io.mohs.rest`), mas nenhuma interface. O projeto irmão Cadrix tem um
dashboard React/TypeScript maduro — design system, tabelas, filtros, drawers,
paleta de comandos — e foi essa base que se decidiu trazer.

O Cadrix, porém, resolve o problema de um jeito que aqui não cabe: o módulo
`cadrix-ui` traz **vinte controllers próprios** (`JobsQueryController`,
`MetricsQueryController`, ...) sob um prefixo `/cadrix-api`. Lá a UI *é* a
API. Aqui já existe uma, com contrato, cursor, RFC 7807 e testes.

Além disso o vocabulário não é o mesmo. O Cadrix tem Queues, Calendars,
Workers e `jobId` opaco; o Mohs removeu a fila (ADR-0021), não tem calendário,
chama de node/runner, e a identidade do job é a própria `jobKey`.

## Decision
Módulo **`mohs-ui`**: um jar que contém apenas o bundle do dashboard em
`classpath:/mohs-ui-webapp`, sem uma linha de Java.

1. **O dashboard consome `/api/mohs/v1`** — a API pública da ADR-0010 — e não
   uma API própria. Duplicar a superfície operacional significaria dois
   contratos para versionar, dois lugares para autorizar e dois lugares onde o
   `ProblemDetail` pode divergir.
2. **Quatro páginas, só as que têm dado real:** Overview, Jobs, Executions,
   Rate Limits. Queues e Calendars não existem no Mohs; `/runners` ainda é
   stub 501; `GET /batches` não existe (só busca por id). Página sem dado é
   dívida, não entrega — as que faltam entram quando o endpoint existir.

   > **Nota (2026-08-21):** `GET /runners` foi implementado e a quinta página
   > entrou, exatamente por essa regra. É a primeira com **cadência própria**:
   > em vez de ser empurrada pelo stream (Overview/Jobs/Executions) ou buscada
   > sob demanda (Rate Limits), ela faz polling de 2s enquanto está aberta.
   > O motivo não é custo — é significado: ocupação de runner é node-local, e
   > um canal que promete visão de cluster entregaria um número que depende de
   > qual nó atendeu o SSE. Ver `../DASHBOARD-STREAM-REVIEW.md` §5.
   > `GET /batches` continua inexistente, e a página de Batches com ele.
3. **O gate é o bundle, não uma classe.** `MohsUiAutoConfiguration` mora no
   starter e liga por `@ConditionalOnResource` sobre o `index.html`. O starter
   **não** depende de `mohs-ui`: quem quer o dashboard declara
   `io.mohs:mohs-ui`, e quem não quer não carrega ~1 MB de JS no jar. É por
   isso que o módulo não precisa de classe marcadora.
4. **Servido sob `/mohs-ui`, em localização própria do classpath** — nunca em
   `classpath:/static`, que o Boot serve na raiz e colidiria com o que o
   hospedeiro já serve lá. Sempre no servidor do próprio hospedeiro: um
   servidor nosso ficaria fora da cadeia de filtros do Spring Security dele, e
   um aplicativo cuidadosamente protegido ainda assim exporia
   pause/cancel/retry numa porta lateral.
5. **Tempo real por SSE.** O Cadrix invalida tudo num timer de 15s porque não
   tem sinal do servidor; o Mohs tem `GET /overview/stream`, que empurra
   `overview`/`jobs`/`nodes`/`executions` a cada 2s com conflation por cliente.
   Os três primeiros frames são semeados direto no cache do React Query — o
   frame carrega o dado, não um aviso de que mudou. O quarto só invalida: ele
   traz a primeira página **sem filtro**, e a tela quase sempre está filtrada.
   O polling de 15s sobrevive apenas como fallback de stream fechado.
6. **Prosa do frontend em inglês**, divergindo da convenção de português do
   CLAUDE.md. Sessenta arquivos vieram do Cadrix já em inglês; traduzir todos
   seria churn sem ganho, e misturar os dois idiomas no mesmo subdiretório é
   pior que qualquer das duas escolhas. A convenção portuguesa segue valendo
   para todo o Java.

## Consequences
O que se paga:

- Um décimo módulo, e o build passa a depender de Node/npm. Mitigado por
  `-Dskip.frontend=true`, que pula o npm inteiro para rodadas backend-only —
  mas o jar publicado nunca pode ser construído assim: sairia sem o dashboard.
- O bundle é um único chunk de ~1 MB (300 kB gzip). Aceitável para uma tela de
  operação interna; code splitting quando incomodar, não antes.
- Os limiares de frescor de heartbeat (`nodeStatus.ts`) são inferidos no
  cliente a partir dos defaults do Mohs (`poll-interval` 5s, `lease-ttl` 30s).
  Um host que suba essas properties verá "Stale" antes da hora. A alternativa
  seria a API expor os limiares, o que hoje ela não faz.
- **Sem `Cache-Control` explícito** em `/mohs-ui/**`: vale o default do Boot. Os assets são
  hasheados pelo Vite (imutáveis por nome) e o `index.html` não pode ser cacheado — a política
  certa é diferente para cada um, e fixá-la sem medir seria chute. Item aberto, registrado.
- O resource handler roda com `resourceChain(false)`, sem o `CachingResourceResolver`: o
  fallback de SPA faz todo caminho resolver com sucesso, então o cache do Spring (mapa sem TTL
  nem teto, chaveado por path) cresceria sem limite com qualquer crawler batendo em paths
  aleatórios. Recurso imutável dentro do jar não paga esse preço.
- `RescheduleForm` não valida a expressão cron: quem sabe se ela é realizável
  é o servidor, que responde 422 com o motivo. Reimplementar o parser criaria
  uma segunda verdade sobre o que é válido, e a errada seria a do cliente.

O que se ganha, além da tela: o dashboard virou o primeiro consumidor real da
API da ADR-0010, e consumir de verdade é o que revela contrato ruim. Já
revelou um: ver **Errata** abaixo.

## Errata — o bug que a integração encontrou
O dashboard serve o mount pelado (`/mohs-ui`) por um `forward:` de
`ViewControllerRegistry`. Isso quebrou com **500** e
`NoSuchBeanDefinitionException: No bean named 'forward:' available`.

A causa não era do dashboard. `MohsJobScanner.postProcessAfterInitialization`
chamava `beanFactory.isSingleton(beanName)` sem verificar se aquele nome tem
bean definition — e o Spring inicializa `View` pelo **nome da view**
(`UrlBasedViewResolver.applyLifecycleMethods` → `initializeBean(view,
viewName)`), que para um `setViewName("forward:/x")` chega como `"forward:"`.

Ou seja: **qualquer aplicativo hospedeiro com view `forward:` ou `redirect:`
quebrava, só por ter o Mohs no classpath.** Latente desde que o scanner
existe, invisível porque nada no projeto usava view resolvida por nome. O
dashboard foi só o primeiro a pisar.

Corrigido com `containsBean` antes de `isSingleton`, e coberto por
`MohsJobScannerTest.aBeanNameWithoutADefinitionIsProcessedInsteadOfBlowingUp`.
Fica o aprendizado de fronteira: um `BeanPostProcessor` recebe objetos que não
são beans declarados, e supor o contrário é supor demais.
