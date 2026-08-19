#!/usr/bin/env node
// .claude/hooks/require-pipeline.mjs
// Cross-platform (Windows/macOS/Linux) end-of-turn quality gate.
//
// Blocks the stop only for files this session actually edited AND that are
// still uncommitted -- gating on `git status` alone fires on every turn once
// the working tree carries long-lived changes, including read-only turns.
//
//   .java changed                  -> java-refactorer + java-code-reviewer
//   .sql / io/mohs/jdbc/ changed   -> db-tuner, before the review

import { execSync } from "node:child_process";
import { readFileSync } from "node:fs";

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
const cwd = process.env.CLAUDE_PROJECT_DIR || input.cwd || process.cwd();

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

const uncommitted = porcelain
    .split(/\r?\n/)
    .filter((line) => line.trim())
    .map((line) => {
        let p = line.slice(3);
        const rename = p.indexOf(" -> ");
        if (rename !== -1) p = p.slice(rename + 4);
        p = p.trim();
        return p.startsWith('"') && p.endsWith('"') ? p.slice(1, -1) : p;
    });

if (uncommitted.length === 0) process.exit(0);

// Windows paths, Git Bash paths and repo-relative paths all reach us; compare
// them on a single normalized form.
const normalize = (p) =>
    p
        .replace(/\\/g, "/")
        .replace(/^\/([a-z])\//i, "$1:/")
        .toLowerCase();

const EDIT_TOOLS = ["Edit", "Write", "MultiEdit", "NotebookEdit"];
const REDIRECT_TARGET = />>?\s*["']?([\w./\\:-]+\.(?:java|sql))\b/gi;
const SCOPED_PATH = /[\w./\\:-]+\.(?:java|sql)\b/gi;
const FILE_MUTATING_VERB =
    /(sed\s+-i|perl\s+-\w*i\b|\btee\b|\bpatch\b|\bmv\b|\bcp\b|git\s+(apply|checkout|restore|mv)|(Set|Add|Clear)-Content|Out-File|New-Item|Remove-Item)/i;

// A heredoc body is data, not commands: a script being written out cites paths
// it never touches. Drop those bodies before looking for write targets.
function stripHeredocBodies(command) {
    const kept = [];
    let terminator = null;
    for (const line of command.split(/\r?\n/)) {
        if (terminator !== null) {
            if (line.trim() === terminator) terminator = null;
            continue;
        }
        kept.push(line);
        const opener = line.match(/<<-?\s*["']?([A-Za-z_]\w*)["']?/);
        if (opener) terminator = opener[1];
    }
    return kept.join("\n");
}

// Paths a shell command actually writes to: redirection targets, plus the
// arguments of a file-mutating verb. A path merely mentioned is not a write.
function writeTargets(command) {
    const stripped = stripHeredocBodies(command)
        .replace(/\d?>+\s*\/dev\/null/g, "")
        .replace(/\d?>&\d/g, "")
        .replace(/\d?>\s*\$null/gi, "");
    const targets = new Set();
    for (const match of stripped.matchAll(REDIRECT_TARGET)) targets.add(match[1]);
    for (const segment of stripped.split(/[;&|]|\r?\n/)) {
        if (!FILE_MUTATING_VERB.test(segment)) continue;
        for (const match of segment.matchAll(SCOPED_PATH)) targets.add(match[0]);
    }
    return targets;
}

function collectToolUses(node, out) {
    if (Array.isArray(node)) {
        for (const child of node) collectToolUses(child, out);
        return;
    }
    if (node && typeof node === "object") {
        if (node.type === "tool_use" && typeof node.name === "string") out.push(node);
        for (const value of Object.values(node)) collectToolUses(value, out);
    }
}

// Files this session wrote to, as seen in the transcript. Shell edits are
// taken from the write targets of the command, not from every path it cites.
function touchedInSession(transcriptPath) {
    const touched = new Set();
    const raw = readFileSync(transcriptPath, "utf8");
    for (const line of raw.split(/\r?\n/)) {
        if (!line.trim()) continue;
        let entry;
        try {
            entry = JSON.parse(line);
        } catch {
            continue;
        }
        const toolUses = [];
        collectToolUses(entry, toolUses);
        for (const use of toolUses) {
            const args = use.input || {};
            if (EDIT_TOOLS.includes(use.name)) {
                for (const key of ["file_path", "notebook_path"]) {
                    if (typeof args[key] === "string") touched.add(normalize(args[key]));
                }
            } else if (typeof args.command === "string") {
                for (const target of writeTargets(args.command)) touched.add(normalize(target));
            }
        }
    }
    return touched;
}

let touched = null;
try {
    if (typeof input.transcript_path === "string") {
        touched = touchedInSession(input.transcript_path);
    }
} catch {
    // Unreadable transcript: fall back to gating on the working tree alone.
    touched = null;
}

// A dirty path is repo-relative; a touched path may be absolute. Either can be
// the suffix of the other.
function wasTouched(dirtyPath) {
    if (touched === null) return true;
    const dirty = normalize(dirtyPath);
    for (const candidate of touched) {
        if (candidate === dirty) return true;
        if (candidate.endsWith("/" + dirty)) return true;
        if (dirty.endsWith("/" + candidate)) return true;
    }
    return false;
}

const pending = uncommitted.filter(wasTouched);
const java = pending.filter((p) => /\.java$/i.test(p));
const persistence = pending.filter(
    (p) => /\.sql$/i.test(p) || normalize(p).includes("src/main/java/io/mohs/jdbc/")
);

if (java.length === 0 && persistence.length === 0) process.exit(0);

const list = (files) => files.join(", ");
const steps = [];
if (java.length > 0) {
    steps.push(
        `(${steps.length + 1}) invoke the java-refactorer subagent scoped to the changed files (${list(java)})`
    );
}
if (persistence.length > 0) {
    steps.push(
        `(${steps.length + 1}) invoke the db-tuner subagent on the persistence changes (${list(persistence)}) — ` +
            "queries, execution plans and index migrations; proposals it flags for approval go to the user, not straight into the tree"
    );
}
steps.push(`(${steps.length + 1}) invoke the java-code-reviewer subagent on the final diff (git diff HEAD)`);
steps.push(`(${steps.length + 1}) fix any CRITICAL findings and re-review (max 2 cycles)`);
steps.push(`(${steps.length + 1}) include the review verdict in your final summary`);

console.log(
    JSON.stringify({
        decision: "block",
        reason:
            "End-of-task quality gate: this session edited files that are still uncommitted. " +
            `Before finishing: ${steps.join("; ")}. ` +
            "If the pipeline already ran on these exact changes in this turn, just state the verdict and finish.",
    })
);
process.exit(0);
