# Writing Style Guide

## Core Principles

### Conciseness
| Avoid | Prefer |
|-------|--------|
| due to the fact that | because |
| in order to | to |
| at this point in time | now |
| for the purpose of | to, for |
| in the event that | if |
| with respect to | about |

### No Redundancy
| Avoid | Prefer |
|-------|--------|
| past history | history |
| future plans | plans |
| end result | result |
| revert back | revert |
| whether or not | whether |
| join together | join |

### No Filler
| Avoid | Prefer |
|-------|--------|
| has the ability to | can |
| is able to | can |
| make reference to | refer to |
| have a tendency to | tend to |

### No Weak Modifiers
Remove: very, really, quite, extremely, basically, actually
- "very unique" → "unique"
- "really important" → "important"

### Active Voice
Prefer active over passive.
- "the request is parsed by the handler" → "the handler parses the request"
- "errors were logged" → "logged the error"

## Markdown Style

**Headers**: Plain text Title Case (no bold, no numbering)
```markdown
## Getting Started    ✓
## **Getting Started** ✗
## 1. Getting Started  ✗
```

**Code blocks**: Always specify language
**Diagrams**: Use Mermaid, not ASCII art
**File naming**: kebab-case (`getting-started.md`)

## Rust Doc Comments

Rule from CLAUDE.md:
- ` ```no_run ` — Rust examples (skipped by `cargo test`, validated by `cargo doc`)
- ` ```text ` — non-Rust content (ASCII diagrams, pseudo-code, output samples)
- **Never** ` ```ignore `

Example:
```markdown
/// Engine that wraps the storage layer.
///
/// ```no_run
/// let engine = StorageEngine::open(path)?;
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
```

## Code Comments

Default: write no comments. Only when the *why* is non-obvious — hidden constraint, subtle invariant, workaround for a specific bug, surprising behavior. Don't explain *what* (well-named identifiers do that) and don't reference the current task, fix, or caller (those belong in the PR description).

## Commit Messages

Follow [Conventional Commits](https://www.conventionalcommits.org/). Prefixes used in this repo: `feat`, `fix`, `docs`, `style`, `refactor`, `perf`, `test`, `build`, `ci`, `chore`, `dx`, `ai`, `imp`, `release`. See `/Users/evan/Developer/inferadb/CLAUDE.md` for the full type table.

Format: `type(scope): description` — lowercase, imperative, no trailing period.

Examples:
- `feat(raft): add ResolveRegionLeader RPC`
- `fix(storage): handle empty batch writes correctly`
- `ci: improvements`

Breaking changes: `feat(api)!: remove deprecated endpoints` or `BREAKING CHANGE:` footer.
