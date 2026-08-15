---
description: Bounded quality loop — refactor → review → fix → re-review until the verdict is ✅
---
Run a bounded quality loop on the current changes. Scope/notes from the user: $ARGUMENTS

If $ARGUMENTS names files, classes, or packages, restrict the loop to them. Otherwise, scope = all uncommitted `.java` changes (`git status --porcelain`, `git diff --name-only HEAD`). If no `.java` files changed, say so and stop.

## Loop state
Maintain and print this block at the end of EVERY cycle:
- `cycle`: N of MAX_CYCLES
- `verdict`: last review verdict (✅ / ⚠️ / ❌)
- `open`: findings still open, by severity (🔴 n, 🟡 n)
- `delta`: what changed since the previous cycle (fixed / justified / new)

MAX_CYCLES = 3, unless $ARGUMENTS specifies a number (hard cap: 5).

## Cycle 1
1. Invoke the **java-refactorer** subagent on the scoped files — explicit path list in the prompt, behavior preserved, tests green before and after.
2. **Conditional:** if the scoped diff touches persistence (SQL, `*Repository`, `@Query`/JDBC code, migrations), invoke the **db-tuner** subagent on exactly those files — result-equivalent rewrites only; index changes as a new migration; destructive changes are proposals. Its 🔴 findings join the review findings in the loop state.
3. Invoke the **java-code-reviewer** subagent on `git diff HEAD` — same file list plus the task's intent. The reviewer is always the last gate of the cycle, validating refactoring and tuning together.

## Cycles 2..MAX_CYCLES (only while verdict is not ✅)
1. In the main session, fix every 🔴 CRITICAL and every 🟡 IMPORTANT you agree with. For a 🟡 you disagree with, do NOT change code — record a one-line justification.
2. Run the test suite. It must be green before any re-review.
3. Re-invoke the **java-code-reviewer** on the updated diff. Subagents have no memory: include in the prompt (a) the previous cycle's findings, (b) which ones were fixed and how, (c) which were justified away and why. Ask it to confirm resolutions and raise only unresolved or NEW issues.

## Exit conditions — check after every cycle, in this order
1. **SUCCESS** — verdict ✅, or ⚠️ where every remaining 🟡 has a recorded justification → stop, report.
2. **STALLED** — the review returned substantially the same findings as the previous cycle (no progress) → stop immediately and report; do not spend cycles re-arguing style opinions.
3. **DIVERGING** — total findings increased two cycles in a row → stop, revert nothing, report what happened.
4. **BUDGET** — cycle == MAX_CYCLES and verdict still ❌ → stop and hand the open 🔴s to the user with your recommended next step.

## Hard rules
- NEVER weaken, skip, or delete a test to make the loop converge. If a test asserts incidental behavior, flag it, stop the loop, and ask.
- Behavior preserved throughout. A bug discovered mid-loop is reported as 🐛, never silently fixed.
- Each cycle's fixes are small and compilable; suite green before every review.
- Do not re-invoke the java-refactorer after cycle 1 unless the fixes introduced obvious mess AND you say so explicitly — the loop converges by fixing findings, not by re-refactoring forever.

## Final output
Loop report: cycles run, verdict trajectory (e.g., ❌ → ⚠️ → ✅), what was fixed per cycle, justified 🟡s with their one-liners, exit condition hit, and anything left open.