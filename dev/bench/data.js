window.BENCHMARK_DATA = {
  "lastUpdate": 1770667549625,
  "repoUrl": "https://github.com/inferadb/ledger",
  "entries": {
    "InferaDB Ledger Benchmarks": [
      {
        "commit": {
          "author": {
            "email": "hello@evansims.com",
            "name": "Evan Sims",
            "username": "evansims"
          },
          "committer": {
            "email": "hello@evansims.com",
            "name": "Evan Sims",
            "username": "evansims"
          },
          "distinct": true,
          "id": "0f8694d74638f74d5f63bf3c94c48ffcae929231",
          "message": "ci: improvements",
          "timestamp": "2026-02-06T22:06:26-06:00",
          "tree_id": "439f16a613c400ebbe6347465284567f94ff3e5a",
          "url": "https://github.com/inferadb/ledger/commit/0f8694d74638f74d5f63bf3c94c48ffcae929231"
        },
        "date": 1770438129360,
        "tool": "cargo",
        "benches": [
          {
            "name": "btree/point_lookup/sequential/1k",
            "value": 920,
            "range": "± 13",
            "unit": "ns/iter"
          },
          {
            "name": "btree/point_lookup/sequential/10k",
            "value": 1179,
            "range": "± 47",
            "unit": "ns/iter"
          },
          {
            "name": "btree/point_lookup/sequential/100k",
            "value": 1333,
            "range": "± 43",
            "unit": "ns/iter"
          },
          {
            "name": "btree/missing_key/10k_entries",
            "value": 1297,
            "range": "± 5",
            "unit": "ns/iter"
          },
          {
            "name": "btree/batch_insert/size/10",
            "value": 347414,
            "range": "± 23348",
            "unit": "ns/iter"
          },
          {
            "name": "btree/batch_insert/size/100",
            "value": 681143,
            "range": "± 33000",
            "unit": "ns/iter"
          },
          {
            "name": "btree/batch_insert/size/1000",
            "value": 2217044,
            "range": "± 139544",
            "unit": "ns/iter"
          },
          {
            "name": "btree/insert_memory/batch_100",
            "value": 150861,
            "range": "± 14913",
            "unit": "ns/iter"
          },
          {
            "name": "btree/iteration/full_scan/1k",
            "value": 159015,
            "range": "± 6523",
            "unit": "ns/iter"
          },
          {
            "name": "btree/iteration/full_scan/10k",
            "value": 1546814,
            "range": "± 6767",
            "unit": "ns/iter"
          },
          {
            "name": "btree/range_scan/10pct_of_10k",
            "value": 165215,
            "range": "± 5295",
            "unit": "ns/iter"
          },
          {
            "name": "btree/mixed_workload/90r_10w",
            "value": 503082,
            "range": "± 43857",
            "unit": "ns/iter"
          },
          {
            "name": "single_reads/entities/1000",
            "value": 1172,
            "range": "± 6",
            "unit": "ns/iter"
          },
          {
            "name": "single_reads/entities/10000",
            "value": 1529,
            "range": "± 4",
            "unit": "ns/iter"
          },
          {
            "name": "single_reads/entities/100000",
            "value": 1869,
            "range": "± 291",
            "unit": "ns/iter"
          },
          {
            "name": "sequential_reads/100_keys",
            "value": 152460,
            "range": "± 584",
            "unit": "ns/iter"
          },
          {
            "name": "random_reads/100_random_keys",
            "value": 155298,
            "range": "± 960",
            "unit": "ns/iter"
          },
          {
            "name": "multi_vault_reads/10_vaults",
            "value": 16134,
            "range": "± 56",
            "unit": "ns/iter"
          },
          {
            "name": "missing_key_reads/missing_key",
            "value": 1581,
            "range": "± 12",
            "unit": "ns/iter"
          },
          {
            "name": "single_writes/vault/1",
            "value": 281871,
            "range": "± 24119",
            "unit": "ns/iter"
          },
          {
            "name": "single_writes/vault/10",
            "value": 292661,
            "range": "± 44927",
            "unit": "ns/iter"
          },
          {
            "name": "single_writes/vault/100",
            "value": 288511,
            "range": "± 29064",
            "unit": "ns/iter"
          },
          {
            "name": "batch_writes/batch_size/10",
            "value": 666899,
            "range": "± 97823",
            "unit": "ns/iter"
          },
          {
            "name": "batch_writes/batch_size/100",
            "value": 3253751,
            "range": "± 1305564",
            "unit": "ns/iter"
          },
          {
            "name": "batch_writes/batch_size/1000",
            "value": 7461809,
            "range": "± 304403",
            "unit": "ns/iter"
          },
          {
            "name": "state_root/entities/100",
            "value": 5797,
            "range": "± 7",
            "unit": "ns/iter"
          },
          {
            "name": "state_root/entities/1000",
            "value": 5799,
            "range": "± 4",
            "unit": "ns/iter"
          },
          {
            "name": "state_root/entities/10000",
            "value": 5797,
            "range": "± 6",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent_vault_writes/10_vaults",
            "value": 2761848,
            "range": "± 237532",
            "unit": "ns/iter"
          },
          {
            "name": "wide_events/context_creation/new",
            "value": 986,
            "range": "± 30",
            "unit": "ns/iter"
          },
          {
            "name": "wide_events/field_population/minimal_fields",
            "value": 1140,
            "range": "± 3",
            "unit": "ns/iter"
          },
          {
            "name": "wide_events/field_population/all_fields",
            "value": 1391,
            "range": "± 62",
            "unit": "ns/iter"
          },
          {
            "name": "wide_events/sampling/should_sample_success",
            "value": 23,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "wide_events/sampling/should_sample_error",
            "value": 4,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "wide_events/end_to_end/write_request",
            "value": 1329,
            "range": "± 7",
            "unit": "ns/iter"
          },
          {
            "name": "wide_events/end_to_end/read_request",
            "value": 1185,
            "range": "± 3",
            "unit": "ns/iter"
          },
          {
            "name": "wide_events/operation_types/count/1",
            "value": 980,
            "range": "± 53",
            "unit": "ns/iter"
          },
          {
            "name": "wide_events/operation_types/count/5",
            "value": 984,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "wide_events/operation_types/count/10",
            "value": 992,
            "range": "± 5",
            "unit": "ns/iter"
          },
          {
            "name": "wide_events/operation_types/count/20",
            "value": 996,
            "range": "± 12",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "188008098+stepsecurity-app[bot]@users.noreply.github.com",
            "name": "stepsecurity-app[bot]",
            "username": "stepsecurity-app[bot]"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "8a15e2d4ddb9e845b1f9db9cbb58f4b7ac90d382",
          "message": "ci(security): apply security best practices (#30)\n\nSigned-off-by: StepSecurity Bot <bot@stepsecurity.io>\nCo-authored-by: stepsecurity-app[bot] <188008098+stepsecurity-app[bot]@users.noreply.github.com>\nCo-authored-by: Evan Sims <hello@evansims.com>",
          "timestamp": "2026-02-06T23:04:56-06:00",
          "tree_id": "7fc496eb427c1a30dc80ec795737a5d686283bc7",
          "url": "https://github.com/inferadb/ledger/commit/8a15e2d4ddb9e845b1f9db9cbb58f4b7ac90d382"
        },
        "date": 1770441475547,
        "tool": "cargo",
        "benches": [
          {
            "name": "btree/point_lookup/sequential/1k",
            "value": 942,
            "range": "± 25",
            "unit": "ns/iter"
          },
          {
            "name": "btree/point_lookup/sequential/10k",
            "value": 1171,
            "range": "± 44",
            "unit": "ns/iter"
          },
          {
            "name": "btree/point_lookup/sequential/100k",
            "value": 1379,
            "range": "± 205",
            "unit": "ns/iter"
          },
          {
            "name": "btree/missing_key/10k_entries",
            "value": 1323,
            "range": "± 7",
            "unit": "ns/iter"
          },
          {
            "name": "btree/batch_insert/size/10",
            "value": 415447,
            "range": "± 86596",
            "unit": "ns/iter"
          },
          {
            "name": "btree/batch_insert/size/100",
            "value": 773395,
            "range": "± 64163",
            "unit": "ns/iter"
          },
          {
            "name": "btree/batch_insert/size/1000",
            "value": 2278424,
            "range": "± 123927",
            "unit": "ns/iter"
          },
          {
            "name": "btree/insert_memory/batch_100",
            "value": 151622,
            "range": "± 13178",
            "unit": "ns/iter"
          },
          {
            "name": "btree/iteration/full_scan/1k",
            "value": 158681,
            "range": "± 553",
            "unit": "ns/iter"
          },
          {
            "name": "btree/iteration/full_scan/10k",
            "value": 1553083,
            "range": "± 30824",
            "unit": "ns/iter"
          },
          {
            "name": "btree/range_scan/10pct_of_10k",
            "value": 164627,
            "range": "± 11769",
            "unit": "ns/iter"
          },
          {
            "name": "btree/mixed_workload/90r_10w",
            "value": 548355,
            "range": "± 30712",
            "unit": "ns/iter"
          },
          {
            "name": "single_reads/entities/1000",
            "value": 2474,
            "range": "± 12",
            "unit": "ns/iter"
          },
          {
            "name": "single_reads/entities/10000",
            "value": 1552,
            "range": "± 13",
            "unit": "ns/iter"
          },
          {
            "name": "single_reads/entities/100000",
            "value": 1868,
            "range": "± 294",
            "unit": "ns/iter"
          },
          {
            "name": "sequential_reads/100_keys",
            "value": 153773,
            "range": "± 8661",
            "unit": "ns/iter"
          },
          {
            "name": "random_reads/100_random_keys",
            "value": 155853,
            "range": "± 658",
            "unit": "ns/iter"
          },
          {
            "name": "multi_vault_reads/10_vaults",
            "value": 15963,
            "range": "± 114",
            "unit": "ns/iter"
          },
          {
            "name": "missing_key_reads/missing_key",
            "value": 2716,
            "range": "± 10",
            "unit": "ns/iter"
          },
          {
            "name": "single_writes/vault/1",
            "value": 331248,
            "range": "± 57665",
            "unit": "ns/iter"
          },
          {
            "name": "single_writes/vault/10",
            "value": 331645,
            "range": "± 33078",
            "unit": "ns/iter"
          },
          {
            "name": "single_writes/vault/100",
            "value": 330553,
            "range": "± 33298",
            "unit": "ns/iter"
          },
          {
            "name": "batch_writes/batch_size/10",
            "value": 705906,
            "range": "± 63631",
            "unit": "ns/iter"
          },
          {
            "name": "batch_writes/batch_size/100",
            "value": 3247521,
            "range": "± 316218",
            "unit": "ns/iter"
          },
          {
            "name": "batch_writes/batch_size/1000",
            "value": 8300985,
            "range": "± 312069",
            "unit": "ns/iter"
          },
          {
            "name": "state_root/entities/100",
            "value": 5796,
            "range": "± 109",
            "unit": "ns/iter"
          },
          {
            "name": "state_root/entities/1000",
            "value": 5796,
            "range": "± 3",
            "unit": "ns/iter"
          },
          {
            "name": "state_root/entities/10000",
            "value": 5797,
            "range": "± 5",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent_vault_writes/10_vaults",
            "value": 3537615,
            "range": "± 259434",
            "unit": "ns/iter"
          },
          {
            "name": "wide_events/context_creation/new",
            "value": 987,
            "range": "± 5",
            "unit": "ns/iter"
          },
          {
            "name": "wide_events/field_population/minimal_fields",
            "value": 1143,
            "range": "± 6",
            "unit": "ns/iter"
          },
          {
            "name": "wide_events/field_population/all_fields",
            "value": 1401,
            "range": "± 6",
            "unit": "ns/iter"
          },
          {
            "name": "wide_events/sampling/should_sample_success",
            "value": 24,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "wide_events/sampling/should_sample_error",
            "value": 4,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "wide_events/end_to_end/write_request",
            "value": 1330,
            "range": "± 4",
            "unit": "ns/iter"
          },
          {
            "name": "wide_events/end_to_end/read_request",
            "value": 1199,
            "range": "± 4",
            "unit": "ns/iter"
          },
          {
            "name": "wide_events/operation_types/count/1",
            "value": 991,
            "range": "± 3",
            "unit": "ns/iter"
          },
          {
            "name": "wide_events/operation_types/count/5",
            "value": 997,
            "range": "± 8",
            "unit": "ns/iter"
          },
          {
            "name": "wide_events/operation_types/count/10",
            "value": 997,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "wide_events/operation_types/count/20",
            "value": 997,
            "range": "± 2",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "hello@evansims.com",
            "name": "Evan Sims",
            "username": "evansims"
          },
          "committer": {
            "email": "hello@evansims.com",
            "name": "Evan Sims",
            "username": "evansims"
          },
          "distinct": true,
          "id": "b45e5c2390587a7f41024d3eef084c506166b72b",
          "message": "imp(sdk): remove snafu dependency",
          "timestamp": "2026-02-08T16:48:32-06:00",
          "tree_id": "2c39ea934b1195fc3674ffc8b840195660005f06",
          "url": "https://github.com/inferadb/ledger/commit/b45e5c2390587a7f41024d3eef084c506166b72b"
        },
        "date": 1770591694158,
        "tool": "cargo",
        "benches": [
          {
            "name": "btree/point_lookup/sequential/1k",
            "value": 938,
            "range": "± 4",
            "unit": "ns/iter"
          },
          {
            "name": "btree/point_lookup/sequential/10k",
            "value": 1220,
            "range": "± 41",
            "unit": "ns/iter"
          },
          {
            "name": "btree/point_lookup/sequential/100k",
            "value": 1385,
            "range": "± 153",
            "unit": "ns/iter"
          },
          {
            "name": "btree/missing_key/10k_entries",
            "value": 1333,
            "range": "± 5",
            "unit": "ns/iter"
          },
          {
            "name": "btree/batch_insert/size/10",
            "value": 352095,
            "range": "± 34501",
            "unit": "ns/iter"
          },
          {
            "name": "btree/batch_insert/size/100",
            "value": 694948,
            "range": "± 38715",
            "unit": "ns/iter"
          },
          {
            "name": "btree/batch_insert/size/1000",
            "value": 2187898,
            "range": "± 113701",
            "unit": "ns/iter"
          },
          {
            "name": "btree/insert_memory/batch_100",
            "value": 153280,
            "range": "± 13740",
            "unit": "ns/iter"
          },
          {
            "name": "btree/iteration/full_scan/1k",
            "value": 159750,
            "range": "± 726",
            "unit": "ns/iter"
          },
          {
            "name": "btree/iteration/full_scan/10k",
            "value": 1561796,
            "range": "± 4507",
            "unit": "ns/iter"
          },
          {
            "name": "btree/range_scan/10pct_of_10k",
            "value": 165289,
            "range": "± 520",
            "unit": "ns/iter"
          },
          {
            "name": "btree/mixed_workload/90r_10w",
            "value": 500396,
            "range": "± 24481",
            "unit": "ns/iter"
          },
          {
            "name": "single_reads/entities/1000",
            "value": 1178,
            "range": "± 20",
            "unit": "ns/iter"
          },
          {
            "name": "single_reads/entities/10000",
            "value": 1558,
            "range": "± 16",
            "unit": "ns/iter"
          },
          {
            "name": "single_reads/entities/100000",
            "value": 1915,
            "range": "± 258",
            "unit": "ns/iter"
          },
          {
            "name": "sequential_reads/100_keys",
            "value": 154396,
            "range": "± 811",
            "unit": "ns/iter"
          },
          {
            "name": "random_reads/100_random_keys",
            "value": 155912,
            "range": "± 2531",
            "unit": "ns/iter"
          },
          {
            "name": "multi_vault_reads/10_vaults",
            "value": 15513,
            "range": "± 110",
            "unit": "ns/iter"
          },
          {
            "name": "missing_key_reads/missing_key",
            "value": 1399,
            "range": "± 16",
            "unit": "ns/iter"
          },
          {
            "name": "single_writes/vault/1",
            "value": 264232,
            "range": "± 59120",
            "unit": "ns/iter"
          },
          {
            "name": "single_writes/vault/10",
            "value": 263397,
            "range": "± 22706",
            "unit": "ns/iter"
          },
          {
            "name": "single_writes/vault/100",
            "value": 270345,
            "range": "± 16724",
            "unit": "ns/iter"
          },
          {
            "name": "batch_writes/batch_size/10",
            "value": 652246,
            "range": "± 97830",
            "unit": "ns/iter"
          },
          {
            "name": "batch_writes/batch_size/100",
            "value": 3260757,
            "range": "± 484341",
            "unit": "ns/iter"
          },
          {
            "name": "batch_writes/batch_size/1000",
            "value": 7656137,
            "range": "± 285368",
            "unit": "ns/iter"
          },
          {
            "name": "state_root/entities/100",
            "value": 5799,
            "range": "± 100",
            "unit": "ns/iter"
          },
          {
            "name": "state_root/entities/1000",
            "value": 5800,
            "range": "± 20",
            "unit": "ns/iter"
          },
          {
            "name": "state_root/entities/10000",
            "value": 5799,
            "range": "± 6",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent_vault_writes/10_vaults",
            "value": 2903354,
            "range": "± 216965",
            "unit": "ns/iter"
          },
          {
            "name": "wide_events/context_creation/new",
            "value": 987,
            "range": "± 5",
            "unit": "ns/iter"
          },
          {
            "name": "wide_events/field_population/minimal_fields",
            "value": 1139,
            "range": "± 5",
            "unit": "ns/iter"
          },
          {
            "name": "wide_events/field_population/all_fields",
            "value": 1391,
            "range": "± 10",
            "unit": "ns/iter"
          },
          {
            "name": "wide_events/sampling/should_sample_success",
            "value": 24,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "wide_events/sampling/should_sample_error",
            "value": 4,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "wide_events/end_to_end/write_request",
            "value": 1331,
            "range": "± 5",
            "unit": "ns/iter"
          },
          {
            "name": "wide_events/end_to_end/read_request",
            "value": 1190,
            "range": "± 20",
            "unit": "ns/iter"
          },
          {
            "name": "wide_events/operation_types/count/1",
            "value": 982,
            "range": "± 3",
            "unit": "ns/iter"
          },
          {
            "name": "wide_events/operation_types/count/5",
            "value": 989,
            "range": "± 13",
            "unit": "ns/iter"
          },
          {
            "name": "wide_events/operation_types/count/10",
            "value": 996,
            "range": "± 5",
            "unit": "ns/iter"
          },
          {
            "name": "wide_events/operation_types/count/20",
            "value": 1002,
            "range": "± 5",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "hello@evansims.com",
            "name": "Evan Sims",
            "username": "evansims"
          },
          "committer": {
            "email": "hello@evansims.com",
            "name": "Evan Sims",
            "username": "evansims"
          },
          "distinct": true,
          "id": "d392ce7717a49989695da6f90d2146e1d30a0ebc",
          "message": "test: concurrency stress tests",
          "timestamp": "2026-02-09T13:35:45-06:00",
          "tree_id": "91135b9c3fef6e133bb3700306de62dbdb7538a1",
          "url": "https://github.com/inferadb/ledger/commit/d392ce7717a49989695da6f90d2146e1d30a0ebc"
        },
        "date": 1770667549255,
        "tool": "cargo",
        "benches": [
          {
            "name": "btree/point_lookup/sequential/1k",
            "value": 921,
            "range": "± 11",
            "unit": "ns/iter"
          },
          {
            "name": "btree/point_lookup/sequential/10k",
            "value": 1194,
            "range": "± 52",
            "unit": "ns/iter"
          },
          {
            "name": "btree/point_lookup/sequential/100k",
            "value": 1399,
            "range": "± 37",
            "unit": "ns/iter"
          },
          {
            "name": "btree/missing_key/10k_entries",
            "value": 1354,
            "range": "± 6",
            "unit": "ns/iter"
          },
          {
            "name": "btree/batch_insert/size/10",
            "value": 242141,
            "range": "± 15006",
            "unit": "ns/iter"
          },
          {
            "name": "btree/batch_insert/size/100",
            "value": 561563,
            "range": "± 34683",
            "unit": "ns/iter"
          },
          {
            "name": "btree/batch_insert/size/1000",
            "value": 2236119,
            "range": "± 132311",
            "unit": "ns/iter"
          },
          {
            "name": "btree/insert_memory/batch_100",
            "value": 170804,
            "range": "± 16026",
            "unit": "ns/iter"
          },
          {
            "name": "btree/iteration/full_scan/1k",
            "value": 156085,
            "range": "± 1435",
            "unit": "ns/iter"
          },
          {
            "name": "btree/iteration/full_scan/10k",
            "value": 1599673,
            "range": "± 3502",
            "unit": "ns/iter"
          },
          {
            "name": "btree/range_scan/10pct_of_10k",
            "value": 166002,
            "range": "± 1717",
            "unit": "ns/iter"
          },
          {
            "name": "btree/mixed_workload/90r_10w",
            "value": 373835,
            "range": "± 17923",
            "unit": "ns/iter"
          },
          {
            "name": "single_reads/entities/1000",
            "value": 1123,
            "range": "± 3",
            "unit": "ns/iter"
          },
          {
            "name": "single_reads/entities/10000",
            "value": 1626,
            "range": "± 6",
            "unit": "ns/iter"
          },
          {
            "name": "single_reads/entities/100000",
            "value": 1938,
            "range": "± 256",
            "unit": "ns/iter"
          },
          {
            "name": "sequential_reads/100_keys",
            "value": 160932,
            "range": "± 816",
            "unit": "ns/iter"
          },
          {
            "name": "random_reads/100_random_keys",
            "value": 167083,
            "range": "± 1020",
            "unit": "ns/iter"
          },
          {
            "name": "multi_vault_reads/10_vaults",
            "value": 15929,
            "range": "± 76",
            "unit": "ns/iter"
          },
          {
            "name": "missing_key_reads/missing_key",
            "value": 1378,
            "range": "± 17",
            "unit": "ns/iter"
          },
          {
            "name": "single_writes/vault/1",
            "value": 188283,
            "range": "± 26174",
            "unit": "ns/iter"
          },
          {
            "name": "single_writes/vault/10",
            "value": 184194,
            "range": "± 24478",
            "unit": "ns/iter"
          },
          {
            "name": "single_writes/vault/100",
            "value": 193322,
            "range": "± 30044",
            "unit": "ns/iter"
          },
          {
            "name": "batch_writes/batch_size/10",
            "value": 662049,
            "range": "± 188035",
            "unit": "ns/iter"
          },
          {
            "name": "batch_writes/batch_size/100",
            "value": 3267059,
            "range": "± 850048",
            "unit": "ns/iter"
          },
          {
            "name": "batch_writes/batch_size/1000",
            "value": 7727421,
            "range": "± 278058",
            "unit": "ns/iter"
          },
          {
            "name": "state_root/entities/100",
            "value": 6675,
            "range": "± 5",
            "unit": "ns/iter"
          },
          {
            "name": "state_root/entities/1000",
            "value": 6623,
            "range": "± 16",
            "unit": "ns/iter"
          },
          {
            "name": "state_root/entities/10000",
            "value": 6621,
            "range": "± 27",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent_vault_writes/10_vaults",
            "value": 1894151,
            "range": "± 254953",
            "unit": "ns/iter"
          },
          {
            "name": "wide_events/context_creation/new",
            "value": 1130,
            "range": "± 28",
            "unit": "ns/iter"
          },
          {
            "name": "wide_events/field_population/minimal_fields",
            "value": 1248,
            "range": "± 26",
            "unit": "ns/iter"
          },
          {
            "name": "wide_events/field_population/all_fields",
            "value": 1535,
            "range": "± 9",
            "unit": "ns/iter"
          },
          {
            "name": "wide_events/sampling/should_sample_success",
            "value": 27,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "wide_events/sampling/should_sample_error",
            "value": 4,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "wide_events/end_to_end/write_request",
            "value": 1461,
            "range": "± 4",
            "unit": "ns/iter"
          },
          {
            "name": "wide_events/end_to_end/read_request",
            "value": 1289,
            "range": "± 6",
            "unit": "ns/iter"
          },
          {
            "name": "wide_events/operation_types/count/1",
            "value": 1108,
            "range": "± 40",
            "unit": "ns/iter"
          },
          {
            "name": "wide_events/operation_types/count/5",
            "value": 1111,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "wide_events/operation_types/count/10",
            "value": 1116,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "wide_events/operation_types/count/20",
            "value": 1122,
            "range": "± 42",
            "unit": "ns/iter"
          }
        ]
      }
    ]
  }
}