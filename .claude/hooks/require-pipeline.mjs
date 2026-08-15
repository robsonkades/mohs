#!/usr/bin/env node
// .claude/hooks/require-pipeline.mjs
// Cross-platform (Windows/macOS/Linux) end-of-turn quality gate:
// blocks Claude from finishing while there are uncommitted .java changes,
// instructing it to run the refactor -> review pipeline first.

import { execSync } from "node:child_process";

// Read the hook event JSON from stdin.
const chunks = [];
for await (const chunk of process.stdin) chunks.push(chunk);
let input = {};
try {
    input = JSON.parse(Buffer.concat(chunks).toString("utf8"));
} catch {
    // Unparseable input: fail open (allow the stop).
    process.exit(0);
}

// CRITICAL: prevent infinite loops. If we're already in a forced
// continuation caused by this hook, allow the stop.
if (input.stop_hook_active === true) process.exit(0);

// Env var read in-process — immune to shell expansion differences on Windows.
const cwd = process.env.CLAUDE_PROJECT_DIR || process.cwd();

let porcelain = "";
try {
    porcelain = execSync("git status --porcelain", {
        cwd,
        encoding: "utf8",
        stdio: ["ignore", "pipe", "ignore"],
    });
} catch {
    // Not a git repo / git missing: fail open.
    process.exit(0);
}

// Any uncommitted change touching a .java file? (handles quoted paths too)
if (/\.java"?$/m.test(porcelain)) {
    console.log(
        JSON.stringify({
            decision: "block",
            reason:
                "End-of-task quality gate: there are uncommitted changes to .java files. " +
                "Before finishing: (1) invoke the java-refactorer subagent scoped to the changed files; " +
                "(2) invoke the java-code-reviewer subagent on the final diff (git diff HEAD); " +
                "(3) fix any CRITICAL findings and re-review (max 2 cycles); " +
                "(4) include the review verdict in your final summary. " +
                "If the pipeline already ran on these exact changes in this turn, just state the verdict and finish.",
        })
    );
}
process.exit(0);
