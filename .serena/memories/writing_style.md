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
