# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.1](https://github.com/inferadb/ledger/compare/v0.1.0...v0.1.1) (2026-01-30)


### Features

* add inkwell crate ([9a63759](https://github.com/inferadb/ledger/commit/9a63759637df41966eabd41b26ca62ed331e0e7f))
* batchread, stress test ([3e4a2fe](https://github.com/inferadb/ledger/commit/3e4a2fed5025b8c22efb759886d7b66da0709a46))
* bootstrap improvements ([b3725ba](https://github.com/inferadb/ledger/commit/b3725ba5b677e33cc3bce11d5a1f1a9bf5791240))
* centralized codec module ([4963b71](https://github.com/inferadb/ledger/commit/4963b711e50361b0c4416517811f5fae8ab17176))
* deletion cascade ([719b674](https://github.com/inferadb/ledger/commit/719b674a549447a35ff83ba06288963777083ee7))
* **deploy:** k8s and helm charts ([10c655c](https://github.com/inferadb/ledger/commit/10c655ccac08544f8d86a2768a6a97a7140e4203))
* discovery service updates ([3f34914](https://github.com/inferadb/ledger/commit/3f34914f340abed3611a6e61f6702db60423408a))
* dual-slot commit ([1d397a9](https://github.com/inferadb/ledger/commit/1d397a94dbe8f533c470bae86bd824cd3e9a1d0d))
* error handling improvements ([bc9ce75](https://github.com/inferadb/ledger/commit/bc9ce7506154022d75ca8cc4c8495f21586abe84))
* error handling improvements ([9a0be4a](https://github.com/inferadb/ledger/commit/9a0be4a5642ea5f3d9ac38137da075d79160bed1))
* garbage collection of expired entities ([b6f7d8b](https://github.com/inferadb/ledger/commit/b6f7d8b863badb58f426cc70dc298915e8342969))
* implement consensus service layer ([c1d8287](https://github.com/inferadb/ledger/commit/c1d8287ba44cced74cd14e060cf868fe2b62478e))
* increase page size 4KB → 16KB ([1207abe](https://github.com/inferadb/ledger/commit/1207abe79a2bae1f891010687d3833d598ab648e))
* initial implementation work ([5730e7e](https://github.com/inferadb/ledger/commit/5730e7ed24576113060e8fd09b592d281ba5e31e))
* leader failover, historical reads ([edc43d8](https://github.com/inferadb/ledger/commit/edc43d896223cbe605051ee0c948a2ca5b1952bf))
* learner refresh metrics, snapshotmanager integration ([96fbe96](https://github.com/inferadb/ledger/commit/96fbe9688c82d6a2e852f5e86c6e00760e3197fc))
* logging improvements ([5b9b64b](https://github.com/inferadb/ledger/commit/5b9b64b7eb497bc0a24bab49700836d515e16542))
* logging improvements ([e5888aa](https://github.com/inferadb/ledger/commit/e5888aa61a3c68c69a3fe54a474c6f3c581cdb10))
* logging improvements ([0c50e6b](https://github.com/inferadb/ledger/commit/0c50e6b4c17cabcc0e5730e4084e579dbae3ef1d))
* merkle proof utilities ([6651a19](https://github.com/inferadb/ledger/commit/6651a195e27d4e8191b5f0ef917795a4861ffc30))
* multi-shard fowarding ([fd279de](https://github.com/inferadb/ledger/commit/fd279de83644f8de7aa89f856cb6b1408bdaeb29))
* multi-shard request routing ([0f2d6d1](https://github.com/inferadb/ledger/commit/0f2d6d1810c800d0361685001f1851a3ccefe760))
* org migration ([73a99b0](https://github.com/inferadb/ledger/commit/73a99b041509e1e43fe9b0f54f00ecf67ab72344))
* org suspension ([f3ce4df](https://github.com/inferadb/ledger/commit/f3ce4dfcc9f678f214af38d4bcd2bf9ef953d09f))
* pagination, self-repair ([4122930](https://github.com/inferadb/ledger/commit/41229300bd3187fe7fd06a720e94e51b58fe06e7))
* proto decode observability ([c3ffee9](https://github.com/inferadb/ledger/commit/c3ffee9adf186f9e1dbada2cbd05af6f22a1dfde))
* real-time watchblock api ([a325302](https://github.com/inferadb/ledger/commit/a32530256dbe48a850955a6539f08891a795454c))
* sdk crate foundation ([56a124a](https://github.com/inferadb/ledger/commit/56a124a68989e6723328938aa8b8a4d90d76f8a0))
* sdk ledger discovery, idempotency, streams ([3fb1b23](https://github.com/inferadb/ledger/commit/3fb1b23d7e7bc816d1f99bf796b0106084e553be))
* **sdk:** e2e tests ([1e722e6](https://github.com/inferadb/ledger/commit/1e722e6666ec19414ce122482922dbb0e9b6718d))
* **sdk:** flush on shutdown ([42438b7](https://github.com/inferadb/ledger/commit/42438b7fbc3d33db49b9c0d329ece200b1f5fc50))
* **sdk:** server discovery support ([c392dc6](https://github.com/inferadb/ledger/commit/c392dc67be33668980aba29ca2010df32d4870f1))
* **sdk:** tls configuration ([bd02841](https://github.com/inferadb/ledger/commit/bd028419243744f6c01e4a7209ccd3f1d98a56bb))
* **sdk:** tracing support ([2d9691b](https://github.com/inferadb/ledger/commit/2d9691b00419e9cec953f605989db59d7d41f103))
* snowflake id generation ([526d1aa](https://github.com/inferadb/ledger/commit/526d1aa060f6fa6cbe4d0308c742f1b0871b9f35))
* time-travel index ([761ce53](https://github.com/inferadb/ledger/commit/761ce53566841ec88dee86096d2ae1e60cfe258f))
* transaction isolation ([0b1a332](https://github.com/inferadb/ledger/commit/0b1a3320f877a40cc95e3fa588bd9b960b3bd436))
* unify locks in RaftLogStore ([daa1cad](https://github.com/inferadb/ledger/commit/daa1caddc955a6ff7015fb5cadff8dc5da57468a))


### Improvements

* protocol improvements ([6d7d018](https://github.com/inferadb/ledger/commit/6d7d0186f1c4ffd71126f5033668d5ba0b05b7c8))
* simplify configurations ([492e099](https://github.com/inferadb/ledger/commit/492e099031902d92a55ba8eea5dbb173bc11d3cf))

## [Unreleased]
