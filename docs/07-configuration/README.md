# 7. Configuration

Status: Active · Last Reviewed: 2026-08-29 · Source of Truth: Repository

| Document | What it covers |
| --- | --- |
| [configuration-reference.md](configuration-reference.md) | Every `mohs.*` property, validated relationships, boot warnings, interactions, and the properties that deliberately do not exist |

## The one mandatory property

```yaml
mohs:
  jdbc:
    dialect: postgresql     # h2 | postgresql | mysql | sqlserver
```

Everything else has a default. An unset dialect fails the boot, naming the four valid values —
**it is never auto-detected from the `DataSource`**.

## Two defaults that are decisions

| Property | Default | Why |
| --- | --- | --- |
| `mohs.api.enabled` | **`false`** | The REST API has no authentication. It must be turned on deliberately |
| `@MohsJob(retries)` | **`1`**, not 0 | With zero budget, a reclaimed orphan has nowhere to reschedule and becomes a terminal `FAILED` — silently lost work in exactly the event the product promises to survive |

## Where configuration lives

Mohs deliberately ships **no `application.yaml`** in any published jar. An `application.yaml` at a
library jar's classpath root competes with the host application's own — only one is loaded, decided
by classpath order — and application configuration always belongs to the application.

The development application (`mohs-demo`) sets its local defaults through
`SpringApplication#setDefaultProperties`, which loses to any external source: a developer's file, a
command-line argument, an environment variable.
