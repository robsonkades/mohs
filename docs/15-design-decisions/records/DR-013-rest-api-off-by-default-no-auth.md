# DR-013: The REST API ships unauthenticated and off by default

## Status

Accepted

## Context

An operational API that can pause jobs, cancel executions and change rate limits cluster-wide is
powerful enough that its security is not optional. But Mohs is embedded
([DR-001](DR-001-embedded-library-not-a-server.md)), and an embedded library cannot know the host's
identity model — session, OAuth2 resource server, mTLS, an API gateway, or something bespoke.

Two bad options present themselves: invent an authentication mechanism the host must adopt, or ship an
open API and hope.

## Decision

**Neither is fully taken. Instead:**

1. The API is **off by default** (`mohs.api.enabled=false`).
2. It performs **no authentication and no authorization of its own**.
3. It is served **on the host's own web server**, inside the host's filter chain — deliberately as the
   only mode.
4. Enabling it logs a WARN naming exactly what it can do.
5. Identity for the **audit trail** is a separate concern with its own SPI, `ActorResolver`,
   defaulting to an `X-Mohs-Actor` header and replaceable via `@ConditionalOnMissingBean`.

The boot warning is treated as a designed artefact, not a log line:

> `mohs.api.enabled=true: the operational API is served at /api/mohs/v1 with NO authentication. It
> can cancel, retry and pause jobs, drain nodes and change rate limits. Restrict it to an internal
> network, or put a gateway/mTLS in front of /api/mohs/v1 and /mohs-ui before exposing this
> instance.`

Its Javadoc says why the wording matters: *"the only active guardrail a user reads before exposing the
API"* — it must say what the API **can do** and what to do about it.

## Consequences

### Positive

- **The host's security applies without any integration work.** Mohs' paths are ordinary paths in the
  host's servlet container, so a `SecurityFilterChain` covers them.
- **No parallel identity model to configure, maintain or get wrong.**
- **The default is safe**: an application that adds Mohs and never sets `mohs.api.enabled` has no
  exposed surface at all.
- `ActorResolver` cleanly separates *attribution* from *authentication*, so the audit trail works in v1
  and improves the moment real identity exists.
- **A serving mode that would have been unsafe was explicitly rejected**: a server of Mohs' own would
  sit outside the host's filter chain, and an application that protected itself carefully would still
  expose pause/cancel/retry on a side port.

### Negative

- **If the host forgets, the API is wide open.** A WARN is the only guardrail, and warnings are
  ignorable.
- **`GET /jobs` discloses `handlerType`** — a fully qualified class name, useful to an attacker
  fingerprinting the application.
- **There is no authorization granularity at all**, so read and write cannot be separated without the
  host doing it.
- **No rate limiting** beyond the 64-subscriber SSE cap, which exists as an amplification guard rather
  than as capacity sizing.
- The bundled dashboard **pins `/api/mohs/v1` in compiled JavaScript**, so changing `base-path` breaks
  it — a WARN covers that too, but it constrains anyone who wanted to hide the API behind an obscure
  path.

## The compensating design

Because the API is unauthenticated, the one caller-controlled string that Mohs **persists and writes
into the audit trail** — the actor — is the most carefully validated input in the codebase: length,
control characters, the *complete* bidi family (including U+061C, which falls outside the usual
ranges), zero-width invisibles, the Tags block, and the reserved name `scheduler` in any casing.

The rule's shape is itself a decision: it **denies the threat family rather than allowing a shape**,
because the first version was an allow-list of "printable" and `\p{Print}` in Java is pure US-ASCII —
it rejected "José" in NFD, the em dash, and Arabic-Indic digits. A person's name became a 400.

## Alternatives considered

| Alternative | Why not |
| --- | --- |
| Ship a built-in authentication mechanism | Cannot know the host's identity model; would duplicate or conflict with it |
| Require Spring Security as a dependency | Forces a dependency on applications that do not use it, and still cannot know how they authenticate |
| Serve the API on a separate port | Explicitly rejected: it would sit outside the host's filter chain |
| Enable the API by default | The default would be an open control plane |
| Refuse to start when the API is on and no `SecurityFilterChain` exists | Not detectable reliably from a library, and would break legitimate gateway-fronted deployments |

## Evidence

- `mohs-spring-boot-starter/src/main/java/io/mohs/autoconfigure/MohsRestAutoConfiguration.java` — the
  gate, the WARN, and the `ActorResolver` conditional.
- `mohs-spring-boot-starter/src/main/java/io/mohs/autoconfigure/MohsUiAutoConfiguration.java` — the
  host's-server-only rationale.
- `mohs-rest/src/main/java/io/mohs/rest/HeaderActorResolver.java` — the validation and its reasoning.
- `mohs-rest/src/main/java/io/mohs/rest/error/RestExceptionHandler.java` — scoped to `io.mohs.rest`
  precisely so that enabling the API does not start deciding the host's error handling.
