---
name: audit-claude-md
description: Use when CLAUDE.md files may be stale — after a major refactor, before a release, when onboarding, when an agent just re-discovered an invariant that should have been written down, or when codebase churn has visibly outpaced documentation updates. Audits the root CLAUDE.md, every per-crate CLAUDE.md, all skills, all agents, and both memory stores against current code. Applies fixes in place and surfaces candidate new golden rules.
---

# audit-claude-md

CLAUDE.md files are contracts. When they drift from code, agents inherit the stale claims and propagate them into new work. This skill is the maintenance loop that keeps the contract honest. Run it on a cadence (before releases, after big refactors) or whenever an audit agent surfaces an invariant that wasn't written down.

## When to run

- **Before a release** — catch stale claims before they ship in onboarding docs.
- **After a major refactor** — file moves, crate splits, RPC renames, dependency swaps all invalidate documented paths and symbols.
- **Monthly cadence** — even without a refactor, churn accumulates. A month is usually enough for drift to appear.
- **On-demand triggers**:
  - An audit agent (`unsafe-panic-auditor`, `snafu-error-reviewer`, etc.) surfaces an invariant that isn't captured as a golden rule.
  - A bug review concludes "a rule would have caught this" — write the rule now.
  - A grep for a documented symbol / path returns zero matches.
  - The same mistake is made twice across PRs — it's a pattern worth codifying.

## What to audit

1. **Root `CLAUDE.md`** (`/CLAUDE.md`) — project overview, tech stack, golden rules, conventions, escalation.
2. **Per-crate `CLAUDE.md`** — `crates/<name>/CLAUDE.md` (symlinked to `AGENTS.md`). One per workspace crate.
3. **Skills** (`.claude/skills/<name>/SKILL.md`) — procedural workflows.
4. **Agents** (`.claude/agents/<name>.md`) — proactive audit agents.
5. **Serena memories** (`.serena/memories/*.md`) — project-local, loaded with Serena tools.
6. **Auto-memory** (`~/.claude/projects/<sanitized-cwd>/memory/*.md`) — cross-session, loaded every conversation. `MEMORY.md` is an index and should stay under ~200 lines.

## Workflow

### 1. Gather shared context (main thread)

Before spawning subagents, gather data once so briefings are self-contained:

- `git log --oneline -60` — recent commits surface RPC renames, dep changes, big refactors.
- `git log --since="14 days ago" --name-only --pretty=format:` — file-level churn.
- `ls` the skills/agents/memory directories — know what exists before auditing.
- Resolve ground-truth facts the subagents will verify against: current crate list, number of gRPC services, list of proto services, existence of key files (`ConsensusEngine` location, `StateLayer` trait impl, etc).

This shared prep is non-negotiable — skipping it forces every subagent to re-discover context, burning time and introducing inconsistency.

### 2. Spawn parallel Explore subagents

Spawn **one subagent per scope** in a single message (parallel). Each prompt must be self-contained — subagents do not see conversation history.

Recommended split:

- **Agent A** — root `CLAUDE.md` + 3–4 smaller per-crate docs.
- **Agent B** — the heavy-churn crates (the ones with the most recent file changes).
- **Agent C** — remaining per-crate docs.
- **Agent D** — all skills.
- **Agent E** — all agents.
- **Agent F** — Serena memories + auto-memory (MEMORY.md + topic files).

Each subagent briefing includes:

- Project context in 2–3 sentences (the agent hasn't seen this session).
- Explicit absolute paths of every file to audit.
- What to verify per claim: file paths exist, symbol names exist (grep/find_symbol), invariants match source reality.
- Recent churn context — list the 6–10 commits that most affect their scope.
- **Output format** (below) + a word-count cap.
- **Explicitly tell them to report only**, not edit. Main thread applies fixes.

Use `subagent_type: Explore` with `very thorough` for deep verification, `medium` for straightforward checks.

### 3. Subagent output format (required)

Every subagent returns findings in the same structured shape so main-thread synthesis is pattern-matchable:

```markdown
## <file path>

### Stale (must fix)
- <claim> — reality: <what code actually says>; suggested fix: <concrete change>

### Add (worth adding)
- <topic> — based on <commit / file / invariant>: <one-line rationale>

### Remove (no longer applicable)
- <claim>

### Verified
- <short list of load-bearing claims confirmed accurate>
```

Report only material findings — things that change agent behavior. Skip cosmetic nits.

### 4. Synthesize + verify before editing

Before applying any subagent finding:

- **Trust but verify** — subagents hallucinate sometimes (imaginary files, nonexistent recipes). Cross-check each "stale" claim against code with a targeted `grep` / `find_symbol` before editing. If the report says "line 46 mentions `clean-stale`" but grep returns no match, the subagent was wrong.
- **Pattern-match across reports** — if three subagents independently flag the same path error (e.g. `raft/src/services/` vs `services/src/services/`), it's a cross-cutting bug; fix with a single Grep + consistent Edits.
- **Separate signal from noise** — a finding that updates one sentence is signal; a finding that reshuffles section ordering without new information is noise.

### 5. Apply fixes in batches

Group edits by theme, not by file:

- **Path corrections** (e.g., wrong crate path) — batch as parallel `Edit` calls; each is surgical.
- **Renames** (e.g., old symbol name lingering) — `Grep` the new name everywhere first, then batch.
- **New subsystems to document** — usually Writes, not Edits; batch per-crate.
- **Obsolete claims to remove** — straightforward deletions; batch.

After editing, re-grep for the old claim to confirm the full-file replacement. A single missed occurrence defeats the edit.

### 6. Surface new golden-rule candidates

For every "Add" finding that names a specific file/function/pattern and has an enforcement mechanism, ask:

- **Has this bug class happened more than once?** If yes, it's a rule candidate for the root `CLAUDE.md` golden-rules list.
- **Is it crate-local?** If yes, it's a candidate for the crate's `CLAUDE.md` Local Golden Rules.
- **Is there an existing agent that should enforce it?** If yes, update the agent. If not, consider a new agent.
- **Can it be a hook?** If the violation is mechanically detectable in `.claude/settings.json`, a `PreToolUse` hook is better than a rule.

Document the candidate in the appropriate file with the standard rule shape: **action verb + specific file/symbol/pattern + detection mechanism**. "Write clean code" is not a rule; "`Shard` in `crates/consensus/src/shard.rs` returns `Action` and performs no I/O" is.

### 6b. Surface breaking-change opportunities (don't apply silently)

Audits sometimes reveal that the code could be materially better if a contract were broken — a redundant RPC removed, a signature simplified, a schema consolidated, a dead config option deleted. **Do not silently apply these.** Produce a short "proposed breaking change" note alongside the audit findings:

- What breaks (wire contract, storage layout, public signature, config shape).
- What it's replaced with.
- The migration path.
- Why the tradeoff is worth it.

Hand this to the human operator for approval before applying. Per the root CLAUDE.md escalation rule, beneficial breaking changes are welcome; the rule is pause-and-confirm, never silent.

### 7. Self-update — update the self-update rules

If the audit surfaced a pattern the "When to Add a New Rule" section doesn't cover, update that section too. The audit is itself a source of meta-learning.

## Verification checklist

For each file type, verify at minimum:

### Root `CLAUDE.md`

- Tech stack table matches `Cargo.toml` versions + `rust-toolchain` / `.mise.toml` pinning.
- Repo structure tree matches `ls crates/` output.
- Every golden rule names a specific file, function, or pattern — no vibes.
- Every golden rule names a detection mechanism (hook, agent, CI, review).
- Escalation triggers reference real agents/tools.

### Per-crate `CLAUDE.md`

- Extends root, never relaxes.
- **Load-Bearing Files** entries exist in the crate, and each row's ramification is still accurate (the file hasn't moved, the thing it breaks hasn't moved either). No file is "frozen" — the section is a caution flag, not a prohibition.
- Owned Surface matches `src/lib.rs` / `src/mod.rs` re-exports.
- Every Local Golden Rule names specific crate-local files/symbols.
- Test Patterns match actual test files.

### Skills

- Every file path referenced exists.
- Every symbol/macro/constant referenced can be found via grep.
- Every command (`just foo`) exists in the `Justfile`.
- Procedure steps match current codebase layout (crate names, module paths).

### Agents

- Scope filter (`**/src/` paths, excluded directories) reflects current tree.
- Banned constructs / invariants audited match current code reality.
- Named files/symbols in the "check this" section exist.
- The `description` field accurately describes when the agent fires.

### Memories

- **Auto-memory `MEMORY.md`**: stays under ~200 lines; every linked topic file exists; index entries one line each.
- **Topic files**: no stale paths, no obsolete symbol names, no references to removed dependencies.
- **Serena memories**: any `.serena/memories/` file with path/symbol references is subject to the same verification.

## Common misses

These are drift classes that audits miss unless explicitly looked for:

- **Cross-file path errors** — one stale path appears in 4 files; fixing 3 leaves the 4th to mislead the next agent. Always `Grep` workspace-wide after any path fix.
- **Symbol renames** — a type renamed in code but still referenced by old name in docs. `Grep` for the old name + confirm zero matches.
- **Obsolete dependency claims** — references to a dep that was removed (e.g., the removed `openraft`). Search the `Cargo.toml` workspace to confirm before quoting a dep.
- **Historical notes presented as current** — a memory file that describes "how it works today" but was written 60 days ago. Check file age against last related commit.
- **Load-Bearing file entries whose ramification has moved** — a file listed as load-bearing because of some invariant it owned, but the invariant has since moved to another file. Re-check every row: does the listed reason still live in this file?
- **Rule / enforcement drift** — a rule says "audited by `foo-agent`" but `foo-agent` was renamed or removed.
- **Auto-memory bloat** — PRD task completions accumulate; they're institutional history, but if they reference paths that have since moved, they actively mislead.

## Output — what the audit produces

At the end of a full run you should have:

- Updated root + per-crate `CLAUDE.md` with every stale path/symbol/claim corrected.
- Updated skills / agents / memories with the same.
- A list of **candidate new golden rules** surfaced during the audit — discuss these with the human operator before adding, since they change agent behavior cluster-wide.
- (Optionally) Updated hooks in `.claude/settings.json` if a mechanical invariant can be enforced mechanically instead of by a written rule.

## Anti-patterns

- **Editing without verifying subagent findings.** Subagents hallucinate imaginary files. Always grep before editing.
- **Rewriting a whole CLAUDE.md because "a lot changed."** Surgical edits preserve institutional context; rewrites lose history. Only rewrite when the structure itself is wrong (e.g., the Golden Rules format introduced after an older "index" style).
- **Treating every "Add" finding as a new rule.** Most are informational additions. A rule requires a file/function/pattern + enforcement — if you can't name both, it's not a rule yet.
- **Running the audit in series instead of parallel.** Six subagents in one message cost roughly the same as one; six subagents in six messages is 6× the latency.
- **Trusting the audit as final.** CLAUDE.md drift is continuous. Schedule the next audit when you finish this one.
