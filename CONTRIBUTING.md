# Contributing to InferaDB Ledger

We welcome contributions! Please read the guidelines below.

## Code of Conduct

This project follows the [Contributor Covenant Code of Conduct](CODE_OF_CONDUCT.md). Report unacceptable behavior to [open@inferadb.com](mailto:open@inferadb.com).

## Reporting Issues

- **Bugs**: Search existing issues first. Include version, reproduction steps, expected vs actual behavior, and logs.
- **Features**: Describe the use case, proposed solution, and alternatives.
- **Security**: Email [security@inferadb.com](mailto:security@inferadb.com). Do not open public issues.

## Pull Requests

1. Fork and branch from `main`
2. Follow [Conventional Commits](https://www.conventionalcommits.org/)
3. Run `just check` before submitting (format + lint + test)
4. Update documentation for API or behavior changes
5. Submit with a clear description

**PR title must follow Conventional Commits format** (validated by CI):
- `feat: add user authentication`
- `fix(api): handle empty requests`

## Development Setup

```bash
# Install tools
mise trust && mise install

# Build and test
just build
just test

# Pre-commit validation
just check

# See all commands
just
```

See [README.md](README.md) for prerequisites.

## Review Process

1. CI runs tests, linters, and formatters
2. A maintainer reviews your contribution
3. Address feedback
4. Maintainer merges on approval

## License

Contributions are dual-licensed under [Apache 2.0](LICENSE-APACHE) and [MIT](LICENSE-MIT).

## Questions?

- [Discord](https://discord.gg/inferadb)
- [open@inferadb.com](mailto:open@inferadb.com)
