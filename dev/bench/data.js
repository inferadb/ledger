window.BENCHMARK_DATA = {
  "lastUpdate": 1770745984186,
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
          "id": "0af1cfafda30b26a6cf72f630b5089f2d675c2f8",
          "message": "feat: add `inferadb-ledger-proto` crate",
          "timestamp": "2026-02-09T21:54:37-06:00",
          "tree_id": "834c952369006468f72e19bd6a22b33732280e79",
          "url": "https://github.com/inferadb/ledger/commit/0af1cfafda30b26a6cf72f630b5089f2d675c2f8"
        },
        "date": 1770696618256,
        "tool": "cargo",
        "benches": [
          {
            "name": "btree/point_lookup/sequential/1k",
            "value": 761,
            "range": "± 6",
            "unit": "ns/iter"
          },
          {
            "name": "btree/point_lookup/sequential/10k",
            "value": 987,
            "range": "± 37",
            "unit": "ns/iter"
          },
          {
            "name": "btree/point_lookup/sequential/100k",
            "value": 1163,
            "range": "± 34",
            "unit": "ns/iter"
          },
          {
            "name": "btree/missing_key/10k_entries",
            "value": 1114,
            "range": "± 10",
            "unit": "ns/iter"
          },
          {
            "name": "btree/batch_insert/size/10",
            "value": 254988,
            "range": "± 12215",
            "unit": "ns/iter"
          },
          {
            "name": "btree/batch_insert/size/100",
            "value": 539019,
            "range": "± 23173",
            "unit": "ns/iter"
          },
          {
            "name": "btree/batch_insert/size/1000",
            "value": 1992154,
            "range": "± 70958",
            "unit": "ns/iter"
          },
          {
            "name": "btree/insert_memory/batch_100",
            "value": 148096,
            "range": "± 11077",
            "unit": "ns/iter"
          },
          {
            "name": "btree/iteration/full_scan/1k",
            "value": 137321,
            "range": "± 633",
            "unit": "ns/iter"
          },
          {
            "name": "btree/iteration/full_scan/10k",
            "value": 1419388,
            "range": "± 15408",
            "unit": "ns/iter"
          },
          {
            "name": "btree/range_scan/10pct_of_10k",
            "value": 146274,
            "range": "± 754",
            "unit": "ns/iter"
          },
          {
            "name": "btree/mixed_workload/90r_10w",
            "value": 351365,
            "range": "± 14074",
            "unit": "ns/iter"
          },
          {
            "name": "single_reads/entities/1000",
            "value": 969,
            "range": "± 13",
            "unit": "ns/iter"
          },
          {
            "name": "single_reads/entities/10000",
            "value": 1375,
            "range": "± 4",
            "unit": "ns/iter"
          },
          {
            "name": "single_reads/entities/100000",
            "value": 1593,
            "range": "± 143",
            "unit": "ns/iter"
          },
          {
            "name": "sequential_reads/100_keys",
            "value": 136618,
            "range": "± 409",
            "unit": "ns/iter"
          },
          {
            "name": "random_reads/100_random_keys",
            "value": 135761,
            "range": "± 684",
            "unit": "ns/iter"
          },
          {
            "name": "multi_vault_reads/10_vaults",
            "value": 14655,
            "range": "± 79",
            "unit": "ns/iter"
          },
          {
            "name": "missing_key_reads/missing_key",
            "value": 1193,
            "range": "± 4",
            "unit": "ns/iter"
          },
          {
            "name": "single_writes/vault/1",
            "value": 269910,
            "range": "± 32364",
            "unit": "ns/iter"
          },
          {
            "name": "single_writes/vault/10",
            "value": 267986,
            "range": "± 50749",
            "unit": "ns/iter"
          },
          {
            "name": "single_writes/vault/100",
            "value": 287109,
            "range": "± 35368",
            "unit": "ns/iter"
          },
          {
            "name": "batch_writes/batch_size/10",
            "value": 684892,
            "range": "± 87355",
            "unit": "ns/iter"
          },
          {
            "name": "batch_writes/batch_size/100",
            "value": 3378207,
            "range": "± 403831",
            "unit": "ns/iter"
          },
          {
            "name": "batch_writes/batch_size/1000",
            "value": 7662658,
            "range": "± 922357",
            "unit": "ns/iter"
          },
          {
            "name": "state_root/entities/100",
            "value": 6864,
            "range": "± 9",
            "unit": "ns/iter"
          },
          {
            "name": "state_root/entities/1000",
            "value": 6856,
            "range": "± 9",
            "unit": "ns/iter"
          },
          {
            "name": "state_root/entities/10000",
            "value": 6858,
            "range": "± 25",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent_vault_writes/10_vaults",
            "value": 2435142,
            "range": "± 197901",
            "unit": "ns/iter"
          },
          {
            "name": "wide_events/context_creation/new",
            "value": 479,
            "range": "± 5",
            "unit": "ns/iter"
          },
          {
            "name": "wide_events/field_population/minimal_fields",
            "value": 618,
            "range": "± 4",
            "unit": "ns/iter"
          },
          {
            "name": "wide_events/field_population/all_fields",
            "value": 816,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "wide_events/sampling/should_sample_success",
            "value": 21,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "wide_events/sampling/should_sample_error",
            "value": 2,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "wide_events/end_to_end/write_request",
            "value": 753,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "wide_events/end_to_end/read_request",
            "value": 732,
            "range": "± 3",
            "unit": "ns/iter"
          },
          {
            "name": "wide_events/operation_types/count/1",
            "value": 490,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "wide_events/operation_types/count/5",
            "value": 498,
            "range": "± 3",
            "unit": "ns/iter"
          },
          {
            "name": "wide_events/operation_types/count/10",
            "value": 497,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "wide_events/operation_types/count/20",
            "value": 501,
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
          "id": "86bd549375842983f2f163cc6e23ddcc761ca1fb",
          "message": "docs: add proto crate README",
          "timestamp": "2026-02-09T22:26:46-06:00",
          "tree_id": "e8c166dbdab8e741876bbddc7577b9e11bbb1a2f",
          "url": "https://github.com/inferadb/ledger/commit/86bd549375842983f2f163cc6e23ddcc761ca1fb"
        },
        "date": 1770698460104,
        "tool": "cargo",
        "benches": [
          {
            "name": "btree/point_lookup/sequential/1k",
            "value": 973,
            "range": "± 32",
            "unit": "ns/iter"
          },
          {
            "name": "btree/point_lookup/sequential/10k",
            "value": 1223,
            "range": "± 46",
            "unit": "ns/iter"
          },
          {
            "name": "btree/point_lookup/sequential/100k",
            "value": 1543,
            "range": "± 70",
            "unit": "ns/iter"
          },
          {
            "name": "btree/missing_key/10k_entries",
            "value": 1304,
            "range": "± 3",
            "unit": "ns/iter"
          },
          {
            "name": "btree/batch_insert/size/10",
            "value": 335425,
            "range": "± 30319",
            "unit": "ns/iter"
          },
          {
            "name": "btree/batch_insert/size/100",
            "value": 706411,
            "range": "± 45952",
            "unit": "ns/iter"
          },
          {
            "name": "btree/batch_insert/size/1000",
            "value": 2412136,
            "range": "± 161456",
            "unit": "ns/iter"
          },
          {
            "name": "btree/insert_memory/batch_100",
            "value": 165296,
            "range": "± 14249",
            "unit": "ns/iter"
          },
          {
            "name": "btree/iteration/full_scan/1k",
            "value": 162384,
            "range": "± 333",
            "unit": "ns/iter"
          },
          {
            "name": "btree/iteration/full_scan/10k",
            "value": 1594931,
            "range": "± 3398",
            "unit": "ns/iter"
          },
          {
            "name": "btree/range_scan/10pct_of_10k",
            "value": 166657,
            "range": "± 359",
            "unit": "ns/iter"
          },
          {
            "name": "btree/mixed_workload/90r_10w",
            "value": 528456,
            "range": "± 30681",
            "unit": "ns/iter"
          },
          {
            "name": "single_reads/entities/1000",
            "value": 1128,
            "range": "± 8",
            "unit": "ns/iter"
          },
          {
            "name": "single_reads/entities/10000",
            "value": 1632,
            "range": "± 9",
            "unit": "ns/iter"
          },
          {
            "name": "single_reads/entities/100000",
            "value": 1876,
            "range": "± 249",
            "unit": "ns/iter"
          },
          {
            "name": "sequential_reads/100_keys",
            "value": 157454,
            "range": "± 1135",
            "unit": "ns/iter"
          },
          {
            "name": "random_reads/100_random_keys",
            "value": 154780,
            "range": "± 594",
            "unit": "ns/iter"
          },
          {
            "name": "multi_vault_reads/10_vaults",
            "value": 16197,
            "range": "± 78",
            "unit": "ns/iter"
          },
          {
            "name": "missing_key_reads/missing_key",
            "value": 1412,
            "range": "± 6",
            "unit": "ns/iter"
          },
          {
            "name": "single_writes/vault/1",
            "value": 270285,
            "range": "± 25599",
            "unit": "ns/iter"
          },
          {
            "name": "single_writes/vault/10",
            "value": 275913,
            "range": "± 31778",
            "unit": "ns/iter"
          },
          {
            "name": "single_writes/vault/100",
            "value": 268048,
            "range": "± 24304",
            "unit": "ns/iter"
          },
          {
            "name": "batch_writes/batch_size/10",
            "value": 657350,
            "range": "± 79994",
            "unit": "ns/iter"
          },
          {
            "name": "batch_writes/batch_size/100",
            "value": 3284398,
            "range": "± 614685",
            "unit": "ns/iter"
          },
          {
            "name": "batch_writes/batch_size/1000",
            "value": 7523073,
            "range": "± 278671",
            "unit": "ns/iter"
          },
          {
            "name": "state_root/entities/100",
            "value": 5817,
            "range": "± 58",
            "unit": "ns/iter"
          },
          {
            "name": "state_root/entities/1000",
            "value": 5815,
            "range": "± 4",
            "unit": "ns/iter"
          },
          {
            "name": "state_root/entities/10000",
            "value": 5816,
            "range": "± 3",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent_vault_writes/10_vaults",
            "value": 2732661,
            "range": "± 170776",
            "unit": "ns/iter"
          },
          {
            "name": "wide_events/context_creation/new",
            "value": 976,
            "range": "± 21",
            "unit": "ns/iter"
          },
          {
            "name": "wide_events/field_population/minimal_fields",
            "value": 1108,
            "range": "± 6",
            "unit": "ns/iter"
          },
          {
            "name": "wide_events/field_population/all_fields",
            "value": 1382,
            "range": "± 4",
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
            "value": 1321,
            "range": "± 15",
            "unit": "ns/iter"
          },
          {
            "name": "wide_events/end_to_end/read_request",
            "value": 1190,
            "range": "± 4",
            "unit": "ns/iter"
          },
          {
            "name": "wide_events/operation_types/count/1",
            "value": 966,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "wide_events/operation_types/count/5",
            "value": 968,
            "range": "± 7",
            "unit": "ns/iter"
          },
          {
            "name": "wide_events/operation_types/count/10",
            "value": 975,
            "range": "± 3",
            "unit": "ns/iter"
          },
          {
            "name": "wide_events/operation_types/count/20",
            "value": 980,
            "range": "± 6",
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
          "id": "b08216431e24531a75140e9aa10d7bab15b4ca6c",
          "message": "chore(deps): bump dependencies",
          "timestamp": "2026-02-09T22:43:51-06:00",
          "tree_id": "4fadc013095ddaf065646a46243e1867d8618d5b",
          "url": "https://github.com/inferadb/ledger/commit/b08216431e24531a75140e9aa10d7bab15b4ca6c"
        },
        "date": 1770699593940,
        "tool": "cargo",
        "benches": [
          {
            "name": "btree/point_lookup/sequential/1k",
            "value": 1558,
            "range": "± 51",
            "unit": "ns/iter"
          },
          {
            "name": "btree/point_lookup/sequential/10k",
            "value": 1214,
            "range": "± 57",
            "unit": "ns/iter"
          },
          {
            "name": "btree/point_lookup/sequential/100k",
            "value": 1379,
            "range": "± 37",
            "unit": "ns/iter"
          },
          {
            "name": "btree/missing_key/10k_entries",
            "value": 1412,
            "range": "± 5",
            "unit": "ns/iter"
          },
          {
            "name": "btree/batch_insert/size/10",
            "value": 686546,
            "range": "± 125486",
            "unit": "ns/iter"
          },
          {
            "name": "btree/batch_insert/size/100",
            "value": 782476,
            "range": "± 83186",
            "unit": "ns/iter"
          },
          {
            "name": "btree/batch_insert/size/1000",
            "value": 2530227,
            "range": "± 147017",
            "unit": "ns/iter"
          },
          {
            "name": "btree/insert_memory/batch_100",
            "value": 166605,
            "range": "± 14786",
            "unit": "ns/iter"
          },
          {
            "name": "btree/iteration/full_scan/1k",
            "value": 165644,
            "range": "± 639",
            "unit": "ns/iter"
          },
          {
            "name": "btree/iteration/full_scan/10k",
            "value": 1595555,
            "range": "± 9070",
            "unit": "ns/iter"
          },
          {
            "name": "btree/range_scan/10pct_of_10k",
            "value": 170783,
            "range": "± 957",
            "unit": "ns/iter"
          },
          {
            "name": "btree/mixed_workload/90r_10w",
            "value": 585800,
            "range": "± 151600",
            "unit": "ns/iter"
          },
          {
            "name": "single_reads/entities/1000",
            "value": 1159,
            "range": "± 9",
            "unit": "ns/iter"
          },
          {
            "name": "single_reads/entities/10000",
            "value": 1636,
            "range": "± 58",
            "unit": "ns/iter"
          },
          {
            "name": "single_reads/entities/100000",
            "value": 1920,
            "range": "± 241",
            "unit": "ns/iter"
          },
          {
            "name": "sequential_reads/100_keys",
            "value": 154497,
            "range": "± 16314",
            "unit": "ns/iter"
          },
          {
            "name": "random_reads/100_random_keys",
            "value": 160008,
            "range": "± 1256",
            "unit": "ns/iter"
          },
          {
            "name": "multi_vault_reads/10_vaults",
            "value": 15800,
            "range": "± 136",
            "unit": "ns/iter"
          },
          {
            "name": "missing_key_reads/missing_key",
            "value": 3416,
            "range": "± 17",
            "unit": "ns/iter"
          },
          {
            "name": "single_writes/vault/1",
            "value": 571476,
            "range": "± 147366",
            "unit": "ns/iter"
          },
          {
            "name": "single_writes/vault/10",
            "value": 453669,
            "range": "± 78252",
            "unit": "ns/iter"
          },
          {
            "name": "single_writes/vault/100",
            "value": 436650,
            "range": "± 67013",
            "unit": "ns/iter"
          },
          {
            "name": "batch_writes/batch_size/10",
            "value": 747500,
            "range": "± 80499",
            "unit": "ns/iter"
          },
          {
            "name": "batch_writes/batch_size/100",
            "value": 3335872,
            "range": "± 328055",
            "unit": "ns/iter"
          },
          {
            "name": "batch_writes/batch_size/1000",
            "value": 8448059,
            "range": "± 226123",
            "unit": "ns/iter"
          },
          {
            "name": "state_root/entities/100",
            "value": 5796,
            "range": "± 16",
            "unit": "ns/iter"
          },
          {
            "name": "state_root/entities/1000",
            "value": 5795,
            "range": "± 134",
            "unit": "ns/iter"
          },
          {
            "name": "state_root/entities/10000",
            "value": 5796,
            "range": "± 6",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent_vault_writes/10_vaults",
            "value": 4272790,
            "range": "± 487958",
            "unit": "ns/iter"
          },
          {
            "name": "wide_events/context_creation/new",
            "value": 749,
            "range": "± 22",
            "unit": "ns/iter"
          },
          {
            "name": "wide_events/field_population/minimal_fields",
            "value": 873,
            "range": "± 3",
            "unit": "ns/iter"
          },
          {
            "name": "wide_events/field_population/all_fields",
            "value": 1131,
            "range": "± 3",
            "unit": "ns/iter"
          },
          {
            "name": "wide_events/sampling/should_sample_success",
            "value": 22,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "wide_events/sampling/should_sample_error",
            "value": 2,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "wide_events/end_to_end/write_request",
            "value": 1041,
            "range": "± 7",
            "unit": "ns/iter"
          },
          {
            "name": "wide_events/end_to_end/read_request",
            "value": 1015,
            "range": "± 64",
            "unit": "ns/iter"
          },
          {
            "name": "wide_events/operation_types/count/1",
            "value": 741,
            "range": "± 4",
            "unit": "ns/iter"
          },
          {
            "name": "wide_events/operation_types/count/5",
            "value": 743,
            "range": "± 8",
            "unit": "ns/iter"
          },
          {
            "name": "wide_events/operation_types/count/10",
            "value": 746,
            "range": "± 29",
            "unit": "ns/iter"
          },
          {
            "name": "wide_events/operation_types/count/20",
            "value": 750,
            "range": "± 9",
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
          "id": "40f61c476977641dc7d322ff90c301766d2ebe35",
          "message": "refactor: split up config and log_storage",
          "timestamp": "2026-02-10T00:21:15-06:00",
          "tree_id": "0cad22e34d0a946e0247b098d750a536b4f388b8",
          "url": "https://github.com/inferadb/ledger/commit/40f61c476977641dc7d322ff90c301766d2ebe35"
        },
        "date": 1770705598901,
        "tool": "cargo",
        "benches": [
          {
            "name": "btree/point_lookup/sequential/1k",
            "value": 1414,
            "range": "± 15",
            "unit": "ns/iter"
          },
          {
            "name": "btree/point_lookup/sequential/10k",
            "value": 1225,
            "range": "± 49",
            "unit": "ns/iter"
          },
          {
            "name": "btree/point_lookup/sequential/100k",
            "value": 1471,
            "range": "± 132",
            "unit": "ns/iter"
          },
          {
            "name": "btree/missing_key/10k_entries",
            "value": 1455,
            "range": "± 21",
            "unit": "ns/iter"
          },
          {
            "name": "btree/batch_insert/size/10",
            "value": 685989,
            "range": "± 125942",
            "unit": "ns/iter"
          },
          {
            "name": "btree/batch_insert/size/100",
            "value": 1095886,
            "range": "± 81706",
            "unit": "ns/iter"
          },
          {
            "name": "btree/batch_insert/size/1000",
            "value": 2864662,
            "range": "± 242366",
            "unit": "ns/iter"
          },
          {
            "name": "btree/insert_memory/batch_100",
            "value": 175135,
            "range": "± 16540",
            "unit": "ns/iter"
          },
          {
            "name": "btree/iteration/full_scan/1k",
            "value": 163709,
            "range": "± 587",
            "unit": "ns/iter"
          },
          {
            "name": "btree/iteration/full_scan/10k",
            "value": 1581468,
            "range": "± 4379",
            "unit": "ns/iter"
          },
          {
            "name": "btree/range_scan/10pct_of_10k",
            "value": 169040,
            "range": "± 1235",
            "unit": "ns/iter"
          },
          {
            "name": "btree/mixed_workload/90r_10w",
            "value": 794556,
            "range": "± 93251",
            "unit": "ns/iter"
          },
          {
            "name": "single_reads/entities/1000",
            "value": 1158,
            "range": "± 10",
            "unit": "ns/iter"
          },
          {
            "name": "single_reads/entities/10000",
            "value": 1604,
            "range": "± 9",
            "unit": "ns/iter"
          },
          {
            "name": "single_reads/entities/100000",
            "value": 1915,
            "range": "± 243",
            "unit": "ns/iter"
          },
          {
            "name": "sequential_reads/100_keys",
            "value": 163349,
            "range": "± 931",
            "unit": "ns/iter"
          },
          {
            "name": "random_reads/100_random_keys",
            "value": 167335,
            "range": "± 1021",
            "unit": "ns/iter"
          },
          {
            "name": "multi_vault_reads/10_vaults",
            "value": 16343,
            "range": "± 132",
            "unit": "ns/iter"
          },
          {
            "name": "missing_key_reads/missing_key",
            "value": 1438,
            "range": "± 6",
            "unit": "ns/iter"
          },
          {
            "name": "single_writes/vault/1",
            "value": 400681,
            "range": "± 89570",
            "unit": "ns/iter"
          },
          {
            "name": "single_writes/vault/10",
            "value": 419370,
            "range": "± 57285",
            "unit": "ns/iter"
          },
          {
            "name": "single_writes/vault/100",
            "value": 378227,
            "range": "± 233842",
            "unit": "ns/iter"
          },
          {
            "name": "batch_writes/batch_size/10",
            "value": 1119792,
            "range": "± 105598",
            "unit": "ns/iter"
          },
          {
            "name": "batch_writes/batch_size/100",
            "value": 3362516,
            "range": "± 273854",
            "unit": "ns/iter"
          },
          {
            "name": "batch_writes/batch_size/1000",
            "value": 8691337,
            "range": "± 309861",
            "unit": "ns/iter"
          },
          {
            "name": "state_root/entities/100",
            "value": 5795,
            "range": "± 3",
            "unit": "ns/iter"
          },
          {
            "name": "state_root/entities/1000",
            "value": 5795,
            "range": "± 4",
            "unit": "ns/iter"
          },
          {
            "name": "state_root/entities/10000",
            "value": 5794,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent_vault_writes/10_vaults",
            "value": 4341780,
            "range": "± 388721",
            "unit": "ns/iter"
          },
          {
            "name": "wide_events/context_creation/new",
            "value": 751,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "wide_events/field_population/minimal_fields",
            "value": 884,
            "range": "± 6",
            "unit": "ns/iter"
          },
          {
            "name": "wide_events/field_population/all_fields",
            "value": 1151,
            "range": "± 7",
            "unit": "ns/iter"
          },
          {
            "name": "wide_events/sampling/should_sample_success",
            "value": 22,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "wide_events/sampling/should_sample_error",
            "value": 2,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "wide_events/end_to_end/write_request",
            "value": 1056,
            "range": "± 41",
            "unit": "ns/iter"
          },
          {
            "name": "wide_events/end_to_end/read_request",
            "value": 1027,
            "range": "± 6",
            "unit": "ns/iter"
          },
          {
            "name": "wide_events/operation_types/count/1",
            "value": 754,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "wide_events/operation_types/count/5",
            "value": 756,
            "range": "± 3",
            "unit": "ns/iter"
          },
          {
            "name": "wide_events/operation_types/count/10",
            "value": 760,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "wide_events/operation_types/count/20",
            "value": 766,
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
          "id": "d15882121d47ce3c2137e560a3c90f5d4848a52b",
          "message": "docs: update MANIFEST",
          "timestamp": "2026-02-10T00:51:04-06:00",
          "tree_id": "2ecde227b8f919c63a8f8519a1aa2d8dd98a2edf",
          "url": "https://github.com/inferadb/ledger/commit/d15882121d47ce3c2137e560a3c90f5d4848a52b"
        },
        "date": 1770707088821,
        "tool": "cargo",
        "benches": [
          {
            "name": "btree/point_lookup/sequential/1k",
            "value": 1398,
            "range": "± 8",
            "unit": "ns/iter"
          },
          {
            "name": "btree/point_lookup/sequential/10k",
            "value": 1204,
            "range": "± 54",
            "unit": "ns/iter"
          },
          {
            "name": "btree/point_lookup/sequential/100k",
            "value": 1442,
            "range": "± 96",
            "unit": "ns/iter"
          },
          {
            "name": "btree/missing_key/10k_entries",
            "value": 1385,
            "range": "± 18",
            "unit": "ns/iter"
          },
          {
            "name": "btree/batch_insert/size/10",
            "value": 484688,
            "range": "± 61935",
            "unit": "ns/iter"
          },
          {
            "name": "btree/batch_insert/size/100",
            "value": 929650,
            "range": "± 85708",
            "unit": "ns/iter"
          },
          {
            "name": "btree/batch_insert/size/1000",
            "value": 2598220,
            "range": "± 162362",
            "unit": "ns/iter"
          },
          {
            "name": "btree/insert_memory/batch_100",
            "value": 170421,
            "range": "± 15726",
            "unit": "ns/iter"
          },
          {
            "name": "btree/iteration/full_scan/1k",
            "value": 161464,
            "range": "± 379",
            "unit": "ns/iter"
          },
          {
            "name": "btree/iteration/full_scan/10k",
            "value": 1578135,
            "range": "± 5957",
            "unit": "ns/iter"
          },
          {
            "name": "btree/range_scan/10pct_of_10k",
            "value": 171329,
            "range": "± 984",
            "unit": "ns/iter"
          },
          {
            "name": "btree/mixed_workload/90r_10w",
            "value": 639767,
            "range": "± 69382",
            "unit": "ns/iter"
          },
          {
            "name": "single_reads/entities/1000",
            "value": 2581,
            "range": "± 15",
            "unit": "ns/iter"
          },
          {
            "name": "single_reads/entities/10000",
            "value": 1694,
            "range": "± 22",
            "unit": "ns/iter"
          },
          {
            "name": "single_reads/entities/100000",
            "value": 1934,
            "range": "± 236",
            "unit": "ns/iter"
          },
          {
            "name": "sequential_reads/100_keys",
            "value": 161303,
            "range": "± 820",
            "unit": "ns/iter"
          },
          {
            "name": "random_reads/100_random_keys",
            "value": 163185,
            "range": "± 1329",
            "unit": "ns/iter"
          },
          {
            "name": "multi_vault_reads/10_vaults",
            "value": 20997,
            "range": "± 230",
            "unit": "ns/iter"
          },
          {
            "name": "missing_key_reads/missing_key",
            "value": 1382,
            "range": "± 7",
            "unit": "ns/iter"
          },
          {
            "name": "single_writes/vault/1",
            "value": 417103,
            "range": "± 66419",
            "unit": "ns/iter"
          },
          {
            "name": "single_writes/vault/10",
            "value": 386473,
            "range": "± 80845",
            "unit": "ns/iter"
          },
          {
            "name": "single_writes/vault/100",
            "value": 391139,
            "range": "± 121573",
            "unit": "ns/iter"
          },
          {
            "name": "batch_writes/batch_size/10",
            "value": 692398,
            "range": "± 97933",
            "unit": "ns/iter"
          },
          {
            "name": "batch_writes/batch_size/100",
            "value": 3344993,
            "range": "± 454341",
            "unit": "ns/iter"
          },
          {
            "name": "batch_writes/batch_size/1000",
            "value": 8533214,
            "range": "± 296568",
            "unit": "ns/iter"
          },
          {
            "name": "state_root/entities/100",
            "value": 5794,
            "range": "± 4",
            "unit": "ns/iter"
          },
          {
            "name": "state_root/entities/1000",
            "value": 5796,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "state_root/entities/10000",
            "value": 5794,
            "range": "± 5",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent_vault_writes/10_vaults",
            "value": 3522674,
            "range": "± 531896",
            "unit": "ns/iter"
          },
          {
            "name": "wide_events/context_creation/new",
            "value": 750,
            "range": "± 7",
            "unit": "ns/iter"
          },
          {
            "name": "wide_events/field_population/minimal_fields",
            "value": 884,
            "range": "± 6",
            "unit": "ns/iter"
          },
          {
            "name": "wide_events/field_population/all_fields",
            "value": 1138,
            "range": "± 10",
            "unit": "ns/iter"
          },
          {
            "name": "wide_events/sampling/should_sample_success",
            "value": 22,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "wide_events/sampling/should_sample_error",
            "value": 2,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "wide_events/end_to_end/write_request",
            "value": 1057,
            "range": "± 3",
            "unit": "ns/iter"
          },
          {
            "name": "wide_events/end_to_end/read_request",
            "value": 1028,
            "range": "± 4",
            "unit": "ns/iter"
          },
          {
            "name": "wide_events/operation_types/count/1",
            "value": 749,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "wide_events/operation_types/count/5",
            "value": 751,
            "range": "± 5",
            "unit": "ns/iter"
          },
          {
            "name": "wide_events/operation_types/count/10",
            "value": 754,
            "range": "± 4",
            "unit": "ns/iter"
          },
          {
            "name": "wide_events/operation_types/count/20",
            "value": 761,
            "range": "± 3",
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
          "id": "c577abff7ac4af340b26226671a19a4c6ed9c277",
          "message": "imp: public api documentation",
          "timestamp": "2026-02-10T11:39:21-06:00",
          "tree_id": "6c18589fbc73695f05ea383b28313ea55cbb27ec",
          "url": "https://github.com/inferadb/ledger/commit/c577abff7ac4af340b26226671a19a4c6ed9c277"
        },
        "date": 1770745983609,
        "tool": "cargo",
        "benches": [
          {
            "name": "btree/point_lookup/sequential/1k",
            "value": 1409,
            "range": "± 18",
            "unit": "ns/iter"
          },
          {
            "name": "btree/point_lookup/sequential/10k",
            "value": 1186,
            "range": "± 58",
            "unit": "ns/iter"
          },
          {
            "name": "btree/point_lookup/sequential/100k",
            "value": 1418,
            "range": "± 157",
            "unit": "ns/iter"
          },
          {
            "name": "btree/missing_key/10k_entries",
            "value": 1866,
            "range": "± 27",
            "unit": "ns/iter"
          },
          {
            "name": "btree/batch_insert/size/10",
            "value": 388287,
            "range": "± 36100",
            "unit": "ns/iter"
          },
          {
            "name": "btree/batch_insert/size/100",
            "value": 804463,
            "range": "± 74490",
            "unit": "ns/iter"
          },
          {
            "name": "btree/batch_insert/size/1000",
            "value": 2465724,
            "range": "± 181505",
            "unit": "ns/iter"
          },
          {
            "name": "btree/insert_memory/batch_100",
            "value": 169046,
            "range": "± 17089",
            "unit": "ns/iter"
          },
          {
            "name": "btree/iteration/full_scan/1k",
            "value": 161777,
            "range": "± 1500",
            "unit": "ns/iter"
          },
          {
            "name": "btree/iteration/full_scan/10k",
            "value": 1583212,
            "range": "± 7131",
            "unit": "ns/iter"
          },
          {
            "name": "btree/range_scan/10pct_of_10k",
            "value": 170591,
            "range": "± 1775",
            "unit": "ns/iter"
          },
          {
            "name": "btree/mixed_workload/90r_10w",
            "value": 556731,
            "range": "± 44088",
            "unit": "ns/iter"
          },
          {
            "name": "single_reads/entities/1000",
            "value": 2521,
            "range": "± 14",
            "unit": "ns/iter"
          },
          {
            "name": "single_reads/entities/10000",
            "value": 1661,
            "range": "± 9",
            "unit": "ns/iter"
          },
          {
            "name": "single_reads/entities/100000",
            "value": 1931,
            "range": "± 227",
            "unit": "ns/iter"
          },
          {
            "name": "sequential_reads/100_keys",
            "value": 162837,
            "range": "± 742",
            "unit": "ns/iter"
          },
          {
            "name": "random_reads/100_random_keys",
            "value": 163745,
            "range": "± 664",
            "unit": "ns/iter"
          },
          {
            "name": "multi_vault_reads/10_vaults",
            "value": 16385,
            "range": "± 411",
            "unit": "ns/iter"
          },
          {
            "name": "missing_key_reads/missing_key",
            "value": 1422,
            "range": "± 42",
            "unit": "ns/iter"
          },
          {
            "name": "single_writes/vault/1",
            "value": 327447,
            "range": "± 49673",
            "unit": "ns/iter"
          },
          {
            "name": "single_writes/vault/10",
            "value": 323613,
            "range": "± 36055",
            "unit": "ns/iter"
          },
          {
            "name": "single_writes/vault/100",
            "value": 325932,
            "range": "± 35498",
            "unit": "ns/iter"
          },
          {
            "name": "batch_writes/batch_size/10",
            "value": 747233,
            "range": "± 76005",
            "unit": "ns/iter"
          },
          {
            "name": "batch_writes/batch_size/100",
            "value": 3307157,
            "range": "± 308453",
            "unit": "ns/iter"
          },
          {
            "name": "batch_writes/batch_size/1000",
            "value": 8466686,
            "range": "± 250176",
            "unit": "ns/iter"
          },
          {
            "name": "state_root/entities/100",
            "value": 5795,
            "range": "± 5",
            "unit": "ns/iter"
          },
          {
            "name": "state_root/entities/1000",
            "value": 5805,
            "range": "± 9",
            "unit": "ns/iter"
          },
          {
            "name": "state_root/entities/10000",
            "value": 5805,
            "range": "± 8",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent_vault_writes/10_vaults",
            "value": 3651323,
            "range": "± 505503",
            "unit": "ns/iter"
          },
          {
            "name": "wide_events/context_creation/new",
            "value": 755,
            "range": "± 3",
            "unit": "ns/iter"
          },
          {
            "name": "wide_events/field_population/minimal_fields",
            "value": 888,
            "range": "± 3",
            "unit": "ns/iter"
          },
          {
            "name": "wide_events/field_population/all_fields",
            "value": 1146,
            "range": "± 3",
            "unit": "ns/iter"
          },
          {
            "name": "wide_events/sampling/should_sample_success",
            "value": 22,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "wide_events/sampling/should_sample_error",
            "value": 2,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "wide_events/end_to_end/write_request",
            "value": 1063,
            "range": "± 4",
            "unit": "ns/iter"
          },
          {
            "name": "wide_events/end_to_end/read_request",
            "value": 1029,
            "range": "± 3",
            "unit": "ns/iter"
          },
          {
            "name": "wide_events/operation_types/count/1",
            "value": 754,
            "range": "± 4",
            "unit": "ns/iter"
          },
          {
            "name": "wide_events/operation_types/count/5",
            "value": 758,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "wide_events/operation_types/count/10",
            "value": 760,
            "range": "± 3",
            "unit": "ns/iter"
          },
          {
            "name": "wide_events/operation_types/count/20",
            "value": 768,
            "range": "± 3",
            "unit": "ns/iter"
          }
        ]
      }
    ]
  }
}