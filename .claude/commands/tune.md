---
description: On-demand database tuning — analyze queries and execution plans in a scope, apply safe fixes
---
Run a database tuning pass. Scope/notes from the user: $ARGUMENTS

If $ARGUMENTS names tables, queries, classes, or packages, restrict the analysis to them. Otherwise, scope = persistence code touched by the current uncommitted changes; if there are none, ask the user which area to tune instead of scanning the whole codebase.

1. Invoke the **db-tuner** subagent with the explicit scope. Include in its prompt: the file paths, the intent (what feels slow or what changed), and whether a live database is expected to be reachable (mention docker-compose/Testcontainers setup if the repo has one).
2. When it returns, apply its report:
    - Fixes it already applied (result-equivalent rewrites, new index migrations) → run the test suite to confirm green.
    - **🧨 Proposals requiring approval** → present them to the user with the trade-offs; do NOT apply without an explicit yes.
    - **📏 Measurements needed** → list the exact commands so the user can run them against a real environment.
3. If any fix touched Java code, finish with the standard gate: invoke the **java-code-reviewer** subagent on `git diff HEAD` covering the tuning changes.
4. Final summary: fixes applied with expected impact, proposals awaiting approval, measurements pending, review verdict.