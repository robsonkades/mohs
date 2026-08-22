---
description: End-of-task pipeline — refactor changed Java files, then review the final diff
---
Run the end-of-task quality pipeline on the current changes. Extra scope/notes from the user: $ARGUMENTS

1. List the changed Java files: run `git status --porcelain` and `git diff --name-only HEAD` (include staged and unstaged). If neither `.java` nor persistence files (`.sql`, code under `io.mohs.store.jdbc`) changed, say so and stop.
2. **Only if `.java` files changed** — invoke the **java-refactorer** subagent with the explicit file list, instructing it to refactor only those files, preserve behavior, and run the tests before and after.
3. **Conditional — persistence touched?** If the diff includes SQL, `*Repository` classes, `@Query`/JDBC code, or migration files: invoke the **db-tuner** subagent on exactly those files (result-equivalent rewrites only; index changes as a new migration; destructive changes are proposals, not edits). Skip this step otherwise and say so.
4. **Only if `.java` files changed** — invoke the **java-code-reviewer** subagent on the final diff — it is always the LAST gate, validating refactoring and tuning together. Tell it to inspect `git diff HEAD`, pass the same file list, and summarize the task's intent so it reviews in context.
5. If the review contains 🔴 CRITICAL findings: fix them here in the main session, then re-run the **java-code-reviewer** (maximum 2 fix→review cycles). If criticals persist, stop and report them to the user instead of looping.
6. Finish with a consolidated summary: refactorings applied, tuning fixes and pending measurements/proposals (when step 3 ran), review verdict (✅ / ⚠️ / ❌), and any open 🟡 items with justification.