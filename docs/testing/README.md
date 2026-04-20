# Testing

Audience: **contributors and internal maintainers** verifying Ledger's correctness claims, plus internals-readers evaluating the testing posture of the project.

InferaDB Ledger uses multiple testing strategies layered on top of standard unit tests. This directory documents the advanced ones.

| Doc                            | When to read                                                           |
| ------------------------------ | ---------------------------------------------------------------------- |
| [fuzzing.md](fuzzing.md)       | Running cargo-fuzz targets, investigating crash artifacts, CI workflow |
| [property.md](property.md)     | Writing proptest-based invariants, strategy reuse, iteration tuning    |
| [simulation.md](simulation.md) | Deterministic consensus simulation, seeded repro, fault injection      |

For day-to-day unit and integration tests, see `just --list` and the [Testing Standards section in CLAUDE.md](../../CLAUDE.md#testing-standards).
