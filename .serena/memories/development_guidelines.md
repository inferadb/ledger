# Development Guidelines

## Version Control Policy
**Read-only access** - Do not execute git write operations (add, commit, push, pull, merge, rebase, etc.). The user handles all version control.

Allowed: `git status`, `git log`, `git diff`, `git show`, `git branch` (listing), `git blame`

## Code Quality Standards

### No Legacy Patterns
This is a new product. Every change should be optimal from day one.

**Blocked patterns:**
- Backwards compatibility shims
- Tech debt markers (TODO, FIXME, HACK, workaround)
- Feature flags/toggles
- Deprecation patterns

**Instead:**
- Make breaking changes directly
- Implement correctly now, not later
- Ship the final implementation
- Remove old code entirely

### Implementation Quality (DRY & KISS)
- No duplicated code that should be extracted
- Shared logic properly abstracted
- Solution as simple as possible, but no simpler
- No unnecessary abstractions or indirection
- Clear, readable code

**Either implement something fully and correctly, or don't implement it at all.**

### No Aspirational Code
Never produce incomplete, placeholder, or "aspirational" implementations.

**Blocked patterns:**
- Stub functions with `todo!()` or `unimplemented!()`
- Placeholder comments like "// TODO: implement this"
- Partial implementations that defer work to "later"
- Skeleton code that outlines structure without logic
- Functions that just return dummy values

**Instead:**
- Fully implement every function, method, and feature
- If a feature is too large, break it into smaller complete pieces
- Each piece of code must be production-ready when written
- If you can't implement something fully, discuss scope first

## TDD Requirements
Target: 90%+ coverage

1. **Red**: Tests written BEFORE implementation, initially failing
2. **Green**: Minimal code to make tests pass
3. **Refactor**: Clean up while keeping tests green

Run coverage: `cargo tarpaulin` or `cargo llvm-cov`

## Tooling

### Formatting
Use nightly toolchain for formatting:
```bash
cargo +nightly fmt
```

### Dependencies
When adding dependencies, verify:
- Latest stable version
- Security advisories checked
- License compatibility (MIT/Apache-2.0)
