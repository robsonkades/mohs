---
description: End-of-task pipeline — refactor changed Java files, then review the final diff
---
Run the end-of-task quality pipeline on the current changes. Extra scope/notes from the user: $ARGUMENTS

1. List the changed Java files: run `git status --porcelain` and `git diff --name-only HEAD` (include staged and unstaged). If no `.java` files changed, say so and stop.
2. Invoke the **java-refactorer** subagent with the explicit file list, instructing it to refactor only those files, preserve behavior, and run the tests before and after.
3. After it returns, invoke the **java-code-reviewer** subagent on the final diff. Tell it to inspect `git diff HEAD`, pass the same file list, and summarize the task's intent so it reviews in context.
4. If the review contains 🔴 CRITICAL findings: fix them here in the main session, then re-run the **java-code-reviewer** (maximum 2 fix→review cycles). If criticals persist, stop and report them to the user instead of looping.
5. Finish with a consolidated summary: refactorings applied, review verdict (✅ / ⚠️ / ❌), and any open 🟡 items with justification.