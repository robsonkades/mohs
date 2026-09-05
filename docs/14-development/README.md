# 14. Development

Status: Active · Last Reviewed: 2026-09-04 · Source of Truth: Repository

| Document | What it covers |
| --- | --- |
| [local-development.md](local-development.md) | Prerequisites, build, running the demo, the dashboard loop, tests, debugging, common tasks, the end-of-change checklist |
| [contributing.md](contributing.md) | The engineering bar, workflow, commit and code conventions, testing expectations, the architecture rules you cannot break silently |

## Sixty seconds to a running system

```bash
git clone <repository-url> mohs && cd mohs
./mvnw clean verify -Dskip.frontend=true
./mvnw -pl mohs-demo -am spring-boot:run -Dspring-boot.run.arguments="--mohs.api.enabled=true"
```

Then `http://localhost:8080/api/mohs/v1/jobs`.

For the dashboard, build once **without** `-Dskip.frontend=true`, then open
`http://localhost:8080/mohs-ui`.

## Three things that will bite you first

1. **Docker must be running** for `mohs-store-jdbc` and `mohs-benchmark`. Without it you get
   `Could not initialize class *TestSupport` — environment, not regression.
2. **`npm ci` fails with `EPERM` while `npm run dev` is running.** Use `-Dskip.frontend=true` for
   backend work; that is precisely why the flag exists.
3. **`io.mohs.rest` cannot see the engine.** If a controller needs new data, add a read method to the
   `Mohs` facade. The reactor enforces it: `mohs-rest` declares only `mohs-api`.
