# Security Policy

InferaDB Ledger is a blockchain database for cryptographically verifiable authorization. It is designed to handle PII under strict data-residency rules and to preserve consensus and cryptographic invariants. We take security reports seriously and welcome coordinated disclosure from the research community.

## Reporting a Vulnerability

**For security researchers, operators, and users:** report vulnerabilities through the private channels below. Public channels (GitHub Issues, pull requests, Discord) are not appropriate for security matters.

### Preferred: GitHub Private Vulnerability Reporting

1. Go to the [Security tab](https://github.com/inferadb/ledger/security) of this repository
2. Click **Report a vulnerability**
3. Fill out the form with details

This creates a private advisory where we can collaborate on a fix before public disclosure.

### Alternative: Email

If you cannot use GitHub's reporting (or prefer email):

- **Email:** [security@inferadb.com](mailto:security@inferadb.com)
- **Subject:** `[SECURITY] <brief description>` — optionally prefix severity, e.g. `[SECURITY][CRITICAL]`

### What to Include

The more of the following you can provide, the faster we can triage:

- Affected component (e.g. `crates/services`, `crates/consensus`, SDK, proto surface)
- Affected versions or commit SHA
- Description of the vulnerability and its impact
- Steps to reproduce, ideally with a minimal proof-of-concept
- Your CVSS v3.1 score and vector, if you have one
- Any suggested remediation
- Your preferred credit name / handle / link (or a request to remain anonymous)

### What to Expect

Timelines are measured from the moment your report is received:

| Elapsed from receipt | Action                                        |
| -------------------- | --------------------------------------------- |
| 72 hours             | Acknowledgment of your report                 |
| 10 business days     | Initial assessment and severity determination |
| See below            | Fix and coordinated disclosure                |

See [Severity and Response Timelines](#severity-and-response-timelines) for fix targets.

**If you don't receive acknowledgment within 72 hours,** escalate to [open@inferadb.com](mailto:open@inferadb.com) — the general maintainer inbox, monitored by the same team as `security@` but outside the dedicated security triage queue.

## Trust Model

InferaDB Ledger is designed to run inside a trusted network perimeter. Before reporting, please understand what the system authenticates itself and what it delegates to the deployment operator.

**Ledger authenticates at the application layer:**

- JWT tokens, TOTP codes, and session credentials
- Authorization checks across organizations, vaults, apps, teams, and users
- Audit records (Merkle-chained and tamper-evident via the `_audit:` key family)
- Backup payloads (encrypted at rest via snapshot encryption; SHA-256 checksum detects corruption on restore)

**Ledger delegates to the operator:**

- Transport-layer identity — mTLS, WireGuard, VPC network policy, or equivalent
- Network perimeter enforcement — the gRPC port is intended to be reachable only from authenticated peers
- Secret management — including the email blinding key, region master encryption keys, TLS certificates, and JWT signing keys
- Host security — OS hardening, process isolation, filesystem ACLs

A report that an attacker with direct network access to the gRPC port can send requests without mTLS is **not** a vulnerability — that's the operator's perimeter responsibility. A report that an authenticated caller can escalate privileges, forge JWTs, bypass permission checks, access another tenant's data, or undermine cryptographic guarantees **is** in scope.

See [`docs/architecture/security.md`](docs/architecture/security.md) for detailed operator security guidance.

## Scope

### In Scope

Reports in the following areas are welcome and will be prioritized:

- **Application-layer authentication and authorization bypass** — JWT forgery, TOTP replay, session hijacking, permission escalation, bypass of the `SystemKeys::validate_key_tier` guard, or any path that yields access to data or operations without valid credentials or permissions
- **Data residency violations** — PII written to `KeyTier::Global` keys, cross-region data leakage, or bypass of residency enforcement
- **Cryptographic weaknesses** — flaws in per-vault AES-256-GCM page / WAL encryption, Merkle block chain verification, snapshot encryption, JWT handling, TOTP lifecycle, or key rotation
- **Consensus safety violations** — split-brain, committed-log divergence, unsafe leader election, stale read-index behavior, or any violation of Raft safety guarantees in `crates/consensus` or `crates/raft`
- **Privilege escalation** — across organizations, vaults, apps, teams, or users, including slug/ID confusion at the gRPC boundary
- **Remote code execution, memory safety violations, or supply-chain compromise** affecting the server or SDK
- **Information disclosure** — PII leakage, cross-tenant data exposure, timing side-channels in authorization checks, or credentials in logs, metrics, or errors
- **Secret-material disclosure** that undermines cryptographic guarantees — leakage of the email blinding key, region master encryption keys, or JWT signing keys through logs, metrics, errors, or API responses
- **SDK vulnerabilities** — authentication bypass, token leakage, or incorrect retry / leader-cache behavior that undermines safety
- **Protocol-level issues** in the gRPC surface — missing `ErrorDetails`, malformed status codes that cause unsafe SDK retries, or API-version negotiation bypass
- **Algorithmic amplification attacks** — single or small-count well-formed inputs that trigger disproportionate CPU, memory, or I/O cost due to a code-level flaw (e.g. pathological proto messages, quadratic B+ tree behavior, rate-limiter bypass that escalates cost per request)
- **Backup and restore integrity** — tampering of backup files that yields undetected state divergence on restore, Merkle chain discontinuity after restore, or bypass of backup encryption or signing
- **Audit and compliance bypass** — evasion of `AuditRecord` / `write_audit_record` paths, tampering with `_audit:` records, or `_shred:` crypto-shredding that fails to render erased data irrecoverable

### Out of Scope

- **Volumetric denial-of-service** — floods of otherwise-valid requests, packet-rate attacks, or resource exhaustion achievable by any operator of a busy deployment (see algorithmic amplification above for what *is* in scope)
- **Wire-layer authentication gaps** — the gRPC port accepting connections without mTLS is an operator perimeter responsibility, not a product vulnerability (see [Trust Model](#trust-model))
- Missing security headers or TLS configuration in *example* configs, unless exploitable
- Vulnerabilities in third-party dependencies without a demonstrated impact in InferaDB Ledger — report these upstream first
- Issues requiring physical access to the host, or compromise of the operator's OS or cloud account
- Self-XSS, clickjacking on non-sensitive pages, or missing CSRF on state-changing operations that require an existing valid token
- Output from automated scanners without manual verification and a proof-of-concept
- Reports against test fixtures or test-only code paths in `crates/test-utils`, unless the pattern could be copied into production
- Social engineering of maintainers, contributors, or the community

If you're unsure whether something is in scope, report it and ask — we'd rather triage and close than miss a real issue.

## Known Limitations

InferaDB Ledger is pre-1.0 and actively hardening. We maintain an internal tracker of security findings under remediation.

**Areas currently under active security review** include: JWT assertion validation and timing (`nbf` enforcement and assertion leeway), saga orchestration PII residency, TOTP code lifecycle, and several algorithmic-amplification paths.

Findings in these areas remain welcome — independent rediscovery confirms priority and often surfaces variants we haven't caught. If your report duplicates an active work item, we will:

- Acknowledge within SLA
- Tell you it duplicates active work
- Credit your independent rediscovery in the eventual advisory (with your permission)
- Share expected resolution timing

We update this list as items are resolved or new areas enter active review.

## Severity and Response Timelines

Severity is assessed using the [CVSS v3.1 calculator](https://www.first.org/cvss/calculator/3.1). If you have a pre-computed score, include it in your report — we'll validate and adjust as needed. The following targets apply from the point a vulnerability is confirmed:

| Severity | CVSS Score | Fix Target             | Coordinated Disclosure |
| -------- | ---------- | ---------------------- | ---------------------- |
| Critical | 9.0 – 10.0 | 30 days                | 7 days after patch     |
| High     | 7.0 – 8.9  | 60 days                | 14 days after patch    |
| Medium   | 4.0 – 6.9  | 90 days                | 30 days after patch    |
| Low      | 0.1 – 3.9  | Next scheduled release | With release notes     |

**Active exploitation in the wild** shortens all timelines and may warrant immediate coordinated disclosure with mitigation guidance. **Complex fixes** may require an extension, negotiated with the reporter in writing.

## Disclosure Policy

We follow coordinated vulnerability disclosure modeled on [Google Project Zero's 90-day policy](https://googleprojectzero.blogspot.com/p/vulnerability-disclosure-policy.html), adjusted per severity as above.

**Process:**

1. We acknowledge within 72 hours of receipt
2. We confirm severity within 10 business days
3. We develop and test a fix, keeping you updated at least every two weeks
4. Once the fix ships in a released version, we publish a [GitHub Security Advisory](https://github.com/inferadb/ledger/security/advisories) and request a CVE via GitHub's CNA program
5. With your permission, we credit you in the advisory

**If we miss a target:**

- You may publish your findings once the fix target for the assessed severity has elapsed
- We may request an extension at any time; extensions require mutual written agreement (an email reply is sufficient)
- If we cannot ship a patch in time, we may publish a pre-patch advisory with mitigation guidance — we will coordinate this with you first

**If impact is demonstrated through a third-party dependency flaw,** we will coordinate upstream disclosure with the dependency maintainers before publishing our own advisory. This may extend the disclosure timeline; we will keep you updated.

**Duplicate reports** are handled as described in [Known Limitations](#known-limitations).

**Notifications to affected operators:**

InferaDB Ledger is open-source software. GDPR Art. 33/34 and equivalent breach-notification obligations fall on the operator running a deployment (the data controller). We commit to:

- Publishing security advisories promptly once a fix or mitigation is available
- Including in advisories the scope, impact, and affected data types needed for operators to fulfill their own breach-notification obligations
- Announcing advisories in the [Discord](https://discord.gg/inferadb) announcements channel and on GitHub Releases

We do not maintain a customer list and cannot directly notify every deployment. Operators of production deployments should [watch the repository for security advisories](https://docs.github.com/en/repositories/managing-your-repositorys-settings-and-features/enabling-features-for-your-repository/managing-security-and-analysis-settings-for-your-repository).

## Safe Harbor

Security research that meets **all** of the following conditions is authorized under this policy:

1. **Scope compliance** — the research targets code in this repository or deployments you are authorized to test
2. **No privacy violations** — you do not access, modify, or exfiltrate data belonging to other users or organizations
3. **No service disruption** — you do not conduct volumetric denial-of-service, data destruction, or attacks that degrade service for others
4. **Minimum-necessary exploitation** — you do not exploit beyond what is required to demonstrate impact
5. **Coordinated disclosure** — you report promptly and allow reasonable time to respond before public disclosure
6. **Inadvertent-access handling** — if you unintentionally access data that is not your own, you stop immediately, do not retain or examine it, and disclose the incident in your report

When your research meets these conditions, we will not:

- Pursue civil legal action against you
- Report you to law enforcement
- Treat your activity as a violation of the [Computer Fraud and Abuse Act](https://www.law.cornell.edu/uscode/text/18/1030) (US), the [Computer Misuse Act](https://www.legislation.gov.uk/ukpga/1990/18) (UK), Germany's [§ 202a StGB](https://www.gesetze-im-internet.de/stgb/__202a.html), or equivalent statutes in other jurisdictions
- Pursue trade-secret or proprietary-information claims (including under [18 USC § 1836](https://www.law.cornell.edu/uscode/text/18/1836) or the [EU Trade Secrets Directive 2016/943](https://eur-lex.europa.eu/eli/dir/2016/943/oj)) for material incidentally disclosed to you in the course of authorized research

If you are researching from a jurisdiction whose law we have not named and you are uncertain whether this authorization reaches you, email [security@inferadb.com](mailto:security@inferadb.com) before testing — we'd rather clarify than have you in doubt.

This authorization covers code in the `inferadb/ledger` repository and deployments you are authorized to test. It does not extend to third-party services or infrastructure that happen to run InferaDB.

## Testing Boundaries

**Acceptable:**

- Source code review
- Running InferaDB Ledger on your own infrastructure
- Fuzzing, chaos testing, and proof-of-concept exploits against deployments you are authorized to test (your own, or under a third-party engagement such as a contracted penetration test)
- Reviewing the proto surface, SDK, and gRPC behavior locally

**Not acceptable:**

- Testing against infrastructure you are not authorized to test
- Accessing, modifying, or exfiltrating data belonging to others
- Volumetric denial-of-service testing against shared or public infrastructure
- Social engineering of maintainers, contributors, or the community
- Persistence, lateral movement, or pivoting beyond proof-of-concept

**If you inadvertently access data that is not your own** during testing: stop immediately, do not retain, examine, or transmit the data, and disclose the incident in your report. Acting in accordance with this paragraph preserves your safe-harbor protection.

## Confidentiality

We treat your report as confidential. We will not:

- Share your identity outside the security triage team without your permission
- Share report contents with third parties beyond what is necessary to develop a fix
- Share your report with business partners, investors, or prospective customers before coordinated disclosure without your explicit written permission
- Discuss your report in public channels before the coordinated disclosure date

If coordinating with an affected operator requires sharing your report, we will notify you first and honor your attribution preferences.

## Supported Versions

InferaDB Ledger is pre-1.0. Security fixes are released against:

- The latest **stable** release
- The `main` branch (via canary and nightly builds)

Older versions do not receive backports. Version support policy will evolve as the project matures — see [CONTRIBUTING.md](CONTRIBUTING.md) for the current release process.

## Bug Bounty

**We do not currently offer monetary bug bounties.** We recognize researchers through:

- Credit in the GitHub Security Advisory (with permission)
- Mention in release notes for the fix
- Public acknowledgment in our [Discord](https://discord.gg/inferadb) announcements channel, if you'd like

## Security Updates

Security fixes are released as patch versions and announced via:

- [GitHub Security Advisories](https://github.com/inferadb/ledger/security/advisories)
- Release notes on GitHub Releases
- The [Discord](https://discord.gg/inferadb) announcements channel

Watch the repository and enable security advisory notifications to receive alerts via GitHub.

## References

- [PII.md](PII.md) — data-residency model, key-tier rules, crypto-shredding architecture
- [DESIGN.md](DESIGN.md) — consensus safety guarantees, storage layer, Merkle block chain
- [`docs/architecture/security.md`](docs/architecture/security.md) — operator security guidance
- [`docs/architecture/data-residency.md`](docs/architecture/data-residency.md) — residency enforcement details
- [CONTRIBUTING.md](CONTRIBUTING.md) — release process, version policy
- [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md) — community standards

## Contact

- **Security reports:** [security@inferadb.com](mailto:security@inferadb.com) or GitHub Private Vulnerability Reporting
- **Escalation / no response:** [open@inferadb.com](mailto:open@inferadb.com)
- **General project contact:** [open@inferadb.com](mailto:open@inferadb.com)
