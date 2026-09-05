## What and why

<!-- What does this change do, and why is it needed? Link the issue it resolves, if any. The why
     is the part the diff cannot show. -->

## How was this tested?

<!-- Which tests were added/updated, and which modules' suites did you run? A performance claim
     needs a before/after number. -->

## Checklist

- [ ] Tests added/updated for the behavior changed, and they fail without the change
- [ ] `./mvnw verify` passes with Docker running, so the Testcontainers suites really ran
- [ ] No test was weakened, disabled or deleted to make the build pass
- [ ] Public API, database schema, configuration or metric names: unchanged — or listed in `CHANGELOG.md`
