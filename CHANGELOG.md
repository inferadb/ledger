# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.1](https://github.com/inferadb/ledger/compare/v0.1.0...v0.1.1) (2026-02-28)


### Features

* add `inferadb-ledger-proto` crate ([0af1cfa](https://github.com/inferadb/ledger/commit/0af1cfafda30b26a6cf72f630b5089f2d675c2f8))
* add inkwell crate ([9a63759](https://github.com/inferadb/ledger/commit/9a63759637df41966eabd41b26ca62ed331e0e7f))
* add sys admin user flag ([62ef9b5](https://github.com/inferadb/ledger/commit/62ef9b5fb2f9f07b26b53ee449b02418712f763d))
* AppliedState and Snapshot improvements ([3152881](https://github.com/inferadb/ledger/commit/3152881e3f3499a36ed282e8862aea0e134ade14))
* audit events api ([c267fb7](https://github.com/inferadb/ledger/commit/c267fb72d4e71a248d94914795600c017056bccb))
* automatic recovery improvements ([c966506](https://github.com/inferadb/ledger/commit/c966506db05f8c74dd7fa65c02d0e92e915ca627))
* backgorund integrity scrubber ([195f4d9](https://github.com/inferadb/ledger/commit/195f4d92c6f7955c1d6663e19f5038abdb056dcc))
* background job observability ([25200c3](https://github.com/inferadb/ledger/commit/25200c38233adcbe6dfcb799b7e3e0d8c39a4dca))
* batchread, stress test ([3e4a2fe](https://github.com/inferadb/ledger/commit/3e4a2fed5025b8c22efb759886d7b66da0709a46))
* bloom filters for negative lookups ([f18ea4c](https://github.com/inferadb/ledger/commit/f18ea4c26546a22160e43ad81ec637d1c7bfffb1))
* bootstrap improvements ([b3725ba](https://github.com/inferadb/ledger/commit/b3725ba5b677e33cc3bce11d5a1f1a9bf5791240))
* centralized codec module ([4963b71](https://github.com/inferadb/ledger/commit/4963b711e50361b0c4416517811f5fae8ab17176))
* config validation improvements ([1eed0a8](https://github.com/inferadb/ledger/commit/1eed0a85b7c54cebd3b70878b68869427b3d50c8))
* deletion cascade ([719b674](https://github.com/inferadb/ledger/commit/719b674a549447a35ff83ba06288963777083ee7))
* **deploy:** k8s and helm charts ([10c655c](https://github.com/inferadb/ledger/commit/10c655ccac08544f8d86a2768a6a97a7140e4203))
* deterministic apply-phase timestamps ([afc90f0](https://github.com/inferadb/ledger/commit/afc90f0f02c03cee11b40fe083cec0d037f49f64))
* discovery service updates ([3f34914](https://github.com/inferadb/ledger/commit/3f34914f340abed3611a6e61f6702db60423408a))
* dual-slot commit ([1d397a9](https://github.com/inferadb/ledger/commit/1d397a94dbe8f533c470bae86bd824cd3e9a1d0d))
* enable single term leader in openraft ([630bd0f](https://github.com/inferadb/ledger/commit/630bd0f3231bb5ce736fb8ce6c508de324a3ee3f))
* enhanced error context, connection draining ([07b4e16](https://github.com/inferadb/ledger/commit/07b4e165c0294568dc67b7dbf45bdb5fa0347afd))
* error handling improvements ([bc9ce75](https://github.com/inferadb/ledger/commit/bc9ce7506154022d75ca8cc4c8495f21586abe84))
* error handling improvements ([9a0be4a](https://github.com/inferadb/ledger/commit/9a0be4a5642ea5f3d9ac38137da075d79160bed1))
* forward reads while nodes are catching up ([d08d558](https://github.com/inferadb/ledger/commit/d08d5587f716372367d3cfcee26d048020a8ef15))
* garbage collection of expired entities ([b6f7d8b](https://github.com/inferadb/ledger/commit/b6f7d8b863badb58f426cc70dc298915e8342969))
* GetEvent secondary index ([9e3df06](https://github.com/inferadb/ledger/commit/9e3df069ef5c7725d7c61188957719a5f4d54cb2))
* graceful leader transfer ([da338c4](https://github.com/inferadb/ledger/commit/da338c46e9e042eefe6f27c54a7e37424ffc238a))
* grpc reflection ([c538276](https://github.com/inferadb/ledger/commit/c5382764e09609f7d56da68df194ab80ae175265))
* implement consensus service layer ([c1d8287](https://github.com/inferadb/ledger/commit/c1d8287ba44cced74cd14e060cf868fe2b62478e))
* improve leav_cluster behavior ([036e65a](https://github.com/inferadb/ledger/commit/036e65a36d50c47b7b86c383a8fc0c62cfb99160))
* improvements to cluster lifecycle tests ([02eb814](https://github.com/inferadb/ledger/commit/02eb81421c6c2007867aa99c7fffe80877f5f336))
* improvements to leadership transfers ([7e87289](https://github.com/inferadb/ledger/commit/7e87289230ad95e60a75e14dca370cf89c770746))
* increase page size 4KB → 16KB ([1207abe](https://github.com/inferadb/ledger/commit/1207abe79a2bae1f891010687d3833d598ab648e))
* incremental backups ([9c267df](https://github.com/inferadb/ledger/commit/9c267df4880e559f444ef66068d16bfcd8e152c6))
* initial implementation work ([5730e7e](https://github.com/inferadb/ledger/commit/5730e7ed24576113060e8fd09b592d281ba5e31e))
* IntegrityScrubberJob bootstrap ([25f60f2](https://github.com/inferadb/ledger/commit/25f60f220979693afd0e1b1f289180a57e223b43))
* leader failover, historical reads ([edc43d8](https://github.com/inferadb/ledger/commit/edc43d896223cbe605051ee0c948a2ca5b1952bf))
* learner refresh metrics, snapshotmanager integration ([96fbe96](https://github.com/inferadb/ledger/commit/96fbe9688c82d6a2e852f5e86c6e00760e3197fc))
* logging improvements ([5b9b64b](https://github.com/inferadb/ledger/commit/5b9b64b7eb497bc0a24bab49700836d515e16542))
* logging improvements ([e5888aa](https://github.com/inferadb/ledger/commit/e5888aa61a3c68c69a3fe54a474c6f3c581cdb10))
* logging improvements ([0c50e6b](https://github.com/inferadb/ledger/commit/0c50e6b4c17cabcc0e5730e4084e579dbae3ef1d))
* merkle proof utilities ([6651a19](https://github.com/inferadb/ledger/commit/6651a195e27d4e8191b5f0ef917795a4861ffc30))
* metric cardinality budgets ([507d12f](https://github.com/inferadb/ledger/commit/507d12f9ac4ccc4860b06a452cc47811ba8116b1))
* multi-shard fowarding ([fd279de](https://github.com/inferadb/ledger/commit/fd279de83644f8de7aa89f856cb6b1408bdaeb29))
* multi-shard request routing ([0f2d6d1](https://github.com/inferadb/ledger/commit/0f2d6d1810c800d0361685001f1851a3ccefe760))
* org migration ([73a99b0](https://github.com/inferadb/ledger/commit/73a99b041509e1e43fe9b0f54f00ecf67ab72344))
* org suspension ([f3ce4df](https://github.com/inferadb/ledger/commit/f3ce4dfcc9f678f214af38d4bcd2bf9ef953d09f))
* pagination, self-repair ([4122930](https://github.com/inferadb/ledger/commit/41229300bd3187fe7fd06a720e94e51b58fe06e7))
* per-namespace resource accounting ([ec1a7c9](https://github.com/inferadb/ledger/commit/ec1a7c9ef9e4c56b47de6ca1d19fe93b870f57eb))
* proto decode observability ([c3ffee9](https://github.com/inferadb/ledger/commit/c3ffee9adf186f9e1dbada2cbd05af6f22a1dfde))
* proto deduplication, raft proposal timeout ([b62ccd5](https://github.com/inferadb/ledger/commit/b62ccd5e46f3785c7492dfdbd7599e6154319460))
* public API improvements ([4e4b25b](https://github.com/inferadb/ledger/commit/4e4b25b1897ceb40f062f38e1a05185f2207ff7c))
* public API improvements ([b842f97](https://github.com/inferadb/ledger/commit/b842f97ff23341e7d85a20a748fb018253631114))
* real-time watchblock api ([a325302](https://github.com/inferadb/ledger/commit/a32530256dbe48a850955a6539f08891a795454c))
* SagaOrchestrator, OrphanCleanupJob ([d45dfb8](https://github.com/inferadb/ledger/commit/d45dfb8c515c6cf8eca093692a4026fa81cf4aa7))
* sdk crate foundation ([56a124a](https://github.com/inferadb/ledger/commit/56a124a68989e6723328938aa8b8a4d90d76f8a0))
* sdk ledger discovery, idempotency, streams ([3fb1b23](https://github.com/inferadb/ledger/commit/3fb1b23d7e7bc816d1f99bf796b0106084e553be))
* **sdk:** e2e tests ([1e722e6](https://github.com/inferadb/ledger/commit/1e722e6666ec19414ce122482922dbb0e9b6718d))
* **sdk:** flush on shutdown ([42438b7](https://github.com/inferadb/ledger/commit/42438b7fbc3d33db49b9c0d329ece200b1f5fc50))
* **sdk:** server discovery support ([c392dc6](https://github.com/inferadb/ledger/commit/c392dc67be33668980aba29ca2010df32d4870f1))
* **sdk:** tls configuration ([bd02841](https://github.com/inferadb/ledger/commit/bd028419243744f6c01e4a7209ccd3f1d98a56bb))
* **sdk:** tracing support ([2d9691b](https://github.com/inferadb/ledger/commit/2d9691b00419e9cec953f605989db59d7d41f103))
* server-assigned sequencing ([8960288](https://github.com/inferadb/ledger/commit/89602886b268cd23a1d2f40f86497ed26c6d81cf))
* service layer consolidation, public API refinement ([4c4ad98](https://github.com/inferadb/ledger/commit/4c4ad980e29993419fc751f33674f84e1fd8987c))
* snowflake id generation ([526d1aa](https://github.com/inferadb/ledger/commit/526d1aa060f6fa6cbe4d0308c742f1b0871b9f35))
* time-travel index ([761ce53](https://github.com/inferadb/ledger/commit/761ce53566841ec88dee86096d2ae1e60cfe258f))
* transaction isolation ([0b1a332](https://github.com/inferadb/ledger/commit/0b1a3320f877a40cc95e3fa588bd9b960b3bd436))
* unify locks in RaftLogStore ([daa1cad](https://github.com/inferadb/ledger/commit/daa1caddc955a6ff7015fb5cadff8dc5da57468a))
* wire TieredSnapshotManager into snapshot flow ([e424aa9](https://github.com/inferadb/ledger/commit/e424aa90736aad458f107f9911658f87dd6811d0))


### Bug Fixes

* cluster cold start bootstrap coordination ([ed34e1a](https://github.com/inferadb/ledger/commit/ed34e1ae7e53c2eb8ddaea2431542a5bf2b684af))
* list_relationships key encoding range ([3da92a4](https://github.com/inferadb/ledger/commit/3da92a4fde88b0ad050a88a1fcdd8ffb2ac24ef1))
* mutable org names ([255bf5c](https://github.com/inferadb/ledger/commit/255bf5c87a84c1d15aa06b07c8e730982d314d75))
* pause heartbeats during leader transfer ([42f3a81](https://github.com/inferadb/ledger/commit/42f3a81cb36c89fa887810d4b61925435950cbfa))
* write concurrency race condition ([9de2b61](https://github.com/inferadb/ledger/commit/9de2b6108c950edd5805d703a8268acf9e0cd22b))


### Improvements

* protocol improvements ([6d7d018](https://github.com/inferadb/ledger/commit/6d7d0186f1c4ffd71126f5033668d5ba0b05b7c8))
* public api documentation ([c577abf](https://github.com/inferadb/ledger/commit/c577abff7ac4af340b26226671a19a4c6ed9c277))
* reduce odds of snowflake ID collisions ([8c54f0f](https://github.com/inferadb/ledger/commit/8c54f0f7d0193dc6b0e3ace35f65b1208a03b424))
* **sdk:** remove snafu dependency ([b45e5c2](https://github.com/inferadb/ledger/commit/b45e5c2390587a7f41024d3eef084c506166b72b))
* simplify configurations ([492e099](https://github.com/inferadb/ledger/commit/492e099031902d92a55ba8eea5dbb173bc11d3cf))

## [Unreleased]
