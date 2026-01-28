[Documentation](../README.md) > Development > Release

# Release Process

How releases are created, versioned, and published.

## Overview

Ledger uses [Release Please](https://github.com/googleapis/release-please) for automated releases. Merging to `main` triggers:

1. **Release PR creation** - Bumps version, updates CHANGELOG
2. **On PR merge** - Builds binaries, Docker images, publishes crates

## Version Scheme

[Semantic Versioning](https://semver.org/): `MAJOR.MINOR.PATCH`

- **MAJOR** - Breaking API changes
- **MINOR** - New features, backwards compatible
- **PATCH** - Bug fixes, backwards compatible

## Triggering a Release

### Standard Release (Recommended)

1. Merge PRs to `main` with [Conventional Commits](https://www.conventionalcommits.org/):

   ```
   feat: add new health check endpoint
   fix: correct state root calculation
   feat!: change WriteRequest format  (breaking change)
   ```

2. Release Please creates a release PR with:
   - Version bump based on commit types
   - Updated CHANGELOG.md
   - Updated Cargo.toml versions

3. Review and merge the release PR

4. Automation handles the rest

### Manual/Hotfix Release

For urgent fixes when the standard process is too slow:

```bash
# Trigger release workflow manually
gh workflow run release.yml -f version=v1.2.3
```

## What Gets Published

### Binaries

| Platform     | Asset Name                             |
| ------------ | -------------------------------------- |
| Linux x86_64 | `inferadb-ledger-linux-x86_64.tar.gz`  |
| Linux ARM64  | `inferadb-ledger-linux-aarch64.tar.gz` |
| macOS ARM64  | `inferadb-ledger-macos-aarch64.tar.gz` |

### Docker Images

Published to both registries with identical tags:

| Registry                           | Tags               |
| ---------------------------------- | ------------------ |
| `ghcr.io/inferadb/inferadb-ledger` | `latest`, `v1.2.3` |
| `inferadb/inferadb-ledger`         | `latest`, `v1.2.3` |

Multi-arch manifest supports `linux/amd64` and `linux/arm64`.

### Rust Crates

Published to [crates.io](https://crates.io) in dependency order:

1. `inferadb-ledger-types`
2. `inferadb-ledger-store`
3. `inferadb-ledger-state`
4. `inferadb-ledger-raft`
5. `inferadb-ledger-sdk`

## Supply Chain Security

### SLSA Provenance

All release binaries include [SLSA Level 3](https://slsa.dev/) provenance attestation. Verify with:

```bash
slsa-verifier verify-artifact inferadb-ledger-linux-x86_64.tar.gz \
  --provenance-path multiple.intoto.jsonl \
  --source-uri github.com/inferadb/ledger
```

### SBOM

Software Bill of Materials (SPDX format) attached to each release:

- `sbom-<target>.spdx.json` - Per-binary SBOM
- `docker-sbom.spdx.json` - Docker image SBOM

### Signing

Docker images are signed with [Sigstore](https://www.sigstore.dev/) cosign. Verify with:

```bash
cosign verify ghcr.io/inferadb/inferadb-ledger:v1.2.3 \
  --certificate-identity-regexp="https://github.com/inferadb/ledger" \
  --certificate-oidc-issuer="https://token.actions.githubusercontent.com"
```

## Commit Message Guidelines

Release Please uses commit messages to determine version bumps:

| Commit Type                    | Version Bump | Example                       |
| ------------------------------ | ------------ | ----------------------------- |
| `fix:`                         | PATCH        | `fix: handle empty namespace` |
| `feat:`                        | MINOR        | `feat: add batch write API`   |
| `feat!:` or `BREAKING CHANGE:` | MAJOR        | `feat!: change proto format`  |
| `docs:`, `chore:`, `test:`     | No bump      | `docs: update API reference`  |

### Breaking Changes

Two ways to signal a breaking change:

```
feat!: change WriteRequest format

The operations field is now required.
```

Or with footer:

```
feat: change WriteRequest format

BREAKING CHANGE: The operations field is now required.
```

## Pre-Release Checklist

Before merging a release PR:

- [ ] CHANGELOG.md accurately describes changes
- [ ] Version number is correct
- [ ] All CI checks pass
- [ ] Documentation updated for new features
- [ ] Breaking changes documented with migration guide

## Troubleshooting

### Release PR Not Created

Check that commits follow Conventional Commits format. Non-conforming commits are ignored.

### crates.io Publish Failed

Common causes:

1. **Version already exists** - Crate was already published
2. **Dependency not indexed** - Wait and retry (there's built-in delay)
3. **Token expired** - OIDC token refresh should be automatic

### Docker Build Failed

Check:

1. Dockerfile syntax
2. Build context includes all needed files
3. Base image availability

## Related Files

| File                                   | Purpose                      |
| -------------------------------------- | ---------------------------- |
| `release-please-config.json`           | Release Please configuration |
| `.release-please-manifest.json`        | Current version tracking     |
| `.github/workflows/release-please.yml` | Release PR automation        |
| `.github/workflows/release.yml`        | Build and publish workflow   |
| `CHANGELOG.md`                         | Release history              |
