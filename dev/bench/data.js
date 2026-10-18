window.BENCHMARK_DATA = {
  "lastUpdate": 1792349531941,
  "repoUrl": "https://github.com/tellus-rs/tellus",
  "entries": {
    "Core": [
      {
        "commit": {
          "author": {
            "email": "git@heikoseeberger.de",
            "name": "Heiko Seeberger",
            "username": "hseeberger"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "80e7177421e90a55b20e7cf1cbbc914903cc80ec",
          "message": "Merge pull request #21 from hseeberger/feat/persistence\n\nfeat: add event-sourced persistence",
          "timestamp": "2026-08-26T08:53:42+02:00",
          "tree_id": "98716b2cae996f3fa4c9ad53d94298f8920d71f0",
          "url": "https://github.com/hseeberger/tellus/commit/80e7177421e90a55b20e7cf1cbbc914903cc80ec"
        },
        "date": 1787727379906,
        "tool": "cargo",
        "benches": [
          {
            "name": "flood/unbounded",
            "value": 11282563,
            "range": "± 57976",
            "unit": "ns/iter"
          },
          {
            "name": "flood/bounded",
            "value": 13138624,
            "range": "± 82600",
            "unit": "ns/iter"
          },
          {
            "name": "ping_pong/pairs/1",
            "value": 697160,
            "range": "± 9731",
            "unit": "ns/iter"
          },
          {
            "name": "ping_pong/pairs/4",
            "value": 799337,
            "range": "± 3521",
            "unit": "ns/iter"
          },
          {
            "name": "fan_out/workers/4",
            "value": 5123831,
            "range": "± 133739",
            "unit": "ns/iter"
          },
          {
            "name": "fan_out/workers/16",
            "value": 3498352,
            "range": "± 32679",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "git@heikoseeberger.de",
            "name": "Heiko Seeberger",
            "username": "hseeberger"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "e51adc4128f5c749e297ca3b3cca92e28d707702",
          "message": "Merge pull request #47 from hseeberger/docs/refresh-after-persistence\n\ndocs: refresh docs after persistence landed",
          "timestamp": "2026-08-26T11:11:33+02:00",
          "tree_id": "25dc57378d1d9b392e4be70e040341db6a59bfdb",
          "url": "https://github.com/hseeberger/tellus/commit/e51adc4128f5c749e297ca3b3cca92e28d707702"
        },
        "date": 1787735650736,
        "tool": "cargo",
        "benches": [
          {
            "name": "flood/unbounded",
            "value": 12160345,
            "range": "± 412558",
            "unit": "ns/iter"
          },
          {
            "name": "flood/bounded",
            "value": 13390135,
            "range": "± 112011",
            "unit": "ns/iter"
          },
          {
            "name": "ping_pong/pairs/1",
            "value": 669611,
            "range": "± 40201",
            "unit": "ns/iter"
          },
          {
            "name": "ping_pong/pairs/4",
            "value": 765980,
            "range": "± 48621",
            "unit": "ns/iter"
          },
          {
            "name": "fan_out/workers/4",
            "value": 4879367,
            "range": "± 68115",
            "unit": "ns/iter"
          },
          {
            "name": "fan_out/workers/16",
            "value": 3428354,
            "range": "± 37869",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "git@heikoseeberger.de",
            "name": "Heiko Seeberger",
            "username": "hseeberger"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "122046c900e5309a4567d0a13b3532cb9a4ab17a",
          "message": "Merge pull request #49 from hseeberger/build/publish-persistence-postgres\n\nbuild: publish tellus-persistence-postgres to crates.io",
          "timestamp": "2026-08-26T11:29:52+02:00",
          "tree_id": "29a63385a27e372b211c583741b956b1e1ccbec7",
          "url": "https://github.com/hseeberger/tellus/commit/122046c900e5309a4567d0a13b3532cb9a4ab17a"
        },
        "date": 1787736711858,
        "tool": "cargo",
        "benches": [
          {
            "name": "flood/unbounded",
            "value": 11858411,
            "range": "± 151430",
            "unit": "ns/iter"
          },
          {
            "name": "flood/bounded",
            "value": 11826622,
            "range": "± 142760",
            "unit": "ns/iter"
          },
          {
            "name": "ping_pong/pairs/1",
            "value": 686555,
            "range": "± 10319",
            "unit": "ns/iter"
          },
          {
            "name": "ping_pong/pairs/4",
            "value": 771690,
            "range": "± 4933",
            "unit": "ns/iter"
          },
          {
            "name": "fan_out/workers/4",
            "value": 5601296,
            "range": "± 110149",
            "unit": "ns/iter"
          },
          {
            "name": "fan_out/workers/16",
            "value": 3555818,
            "range": "± 9906",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "git@heikoseeberger.de",
            "name": "Heiko Seeberger",
            "username": "hseeberger"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "0f679455839c1ea6d716352859a2dcd95ed40711",
          "message": "Merge pull request #51 from hseeberger/docs/cargo-add-install\n\ndocs: install via cargo add instead of pinned versions",
          "timestamp": "2026-08-27T17:42:04+02:00",
          "tree_id": "95736cb2e525bf5f787649f3157aa160bfe0b767",
          "url": "https://github.com/hseeberger/tellus/commit/0f679455839c1ea6d716352859a2dcd95ed40711"
        },
        "date": 1787845457377,
        "tool": "cargo",
        "benches": [
          {
            "name": "flood/unbounded",
            "value": 26640174,
            "range": "± 1317423",
            "unit": "ns/iter"
          },
          {
            "name": "flood/bounded",
            "value": 33932148,
            "range": "± 1002903",
            "unit": "ns/iter"
          },
          {
            "name": "ping_pong/pairs/1",
            "value": 915378,
            "range": "± 6953",
            "unit": "ns/iter"
          },
          {
            "name": "ping_pong/pairs/4",
            "value": 1118685,
            "range": "± 37016",
            "unit": "ns/iter"
          },
          {
            "name": "fan_out/workers/4",
            "value": 9509958,
            "range": "± 221441",
            "unit": "ns/iter"
          },
          {
            "name": "fan_out/workers/16",
            "value": 8874525,
            "range": "± 228582",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "git@heikoseeberger.de",
            "name": "Heiko Seeberger",
            "username": "hseeberger"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "779ef2f79d25a8532753cbbb110a8b93ce8bcbb9",
          "message": "Merge pull request #52 from hseeberger/build/independent-crate-versions\n\nbuild: use independent versions per crate",
          "timestamp": "2026-08-27T17:53:17+02:00",
          "tree_id": "bb4b2115dc6995dd7741bc79ba515d70bc94d278",
          "url": "https://github.com/hseeberger/tellus/commit/779ef2f79d25a8532753cbbb110a8b93ce8bcbb9"
        },
        "date": 1787846112870,
        "tool": "cargo",
        "benches": [
          {
            "name": "flood/unbounded",
            "value": 9100745,
            "range": "± 111013",
            "unit": "ns/iter"
          },
          {
            "name": "flood/bounded",
            "value": 11188195,
            "range": "± 103266",
            "unit": "ns/iter"
          },
          {
            "name": "ping_pong/pairs/1",
            "value": 562944,
            "range": "± 4973",
            "unit": "ns/iter"
          },
          {
            "name": "ping_pong/pairs/4",
            "value": 613272,
            "range": "± 3294",
            "unit": "ns/iter"
          },
          {
            "name": "fan_out/workers/4",
            "value": 4339785,
            "range": "± 30941",
            "unit": "ns/iter"
          },
          {
            "name": "fan_out/workers/16",
            "value": 2779179,
            "range": "± 10062",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "git@heikoseeberger.de",
            "name": "Heiko Seeberger",
            "username": "hseeberger"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "105234ca3f6a7f3c36f7bb95f8bb8b1fef19e829",
          "message": "Merge pull request #54 from hseeberger/release/tellus-0.2.0\n\nchore: release tellus 0.2.0 and tellus-persistence-postgres 0.1.0",
          "timestamp": "2026-08-27T23:33:37+02:00",
          "tree_id": "173bf718a78bdb32ab258f04cd95f1f2a59eefda",
          "url": "https://github.com/hseeberger/tellus/commit/105234ca3f6a7f3c36f7bb95f8bb8b1fef19e829"
        },
        "date": 1787866532863,
        "tool": "cargo",
        "benches": [
          {
            "name": "flood/unbounded",
            "value": 9221059,
            "range": "± 174124",
            "unit": "ns/iter"
          },
          {
            "name": "flood/bounded",
            "value": 8928111,
            "range": "± 429757",
            "unit": "ns/iter"
          },
          {
            "name": "ping_pong/pairs/1",
            "value": 548536,
            "range": "± 2599",
            "unit": "ns/iter"
          },
          {
            "name": "ping_pong/pairs/4",
            "value": 604777,
            "range": "± 9378",
            "unit": "ns/iter"
          },
          {
            "name": "fan_out/workers/4",
            "value": 4208362,
            "range": "± 97396",
            "unit": "ns/iter"
          },
          {
            "name": "fan_out/workers/16",
            "value": 2758914,
            "range": "± 13823",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "git@heikoseeberger.de",
            "name": "Heiko Seeberger",
            "username": "hseeberger"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "f7e8a3ec45a0f24d8c8bbc25a3a8f12714245fcf",
          "message": "Merge pull request #55 from hseeberger/chore/bench-variance\n\nchore: pin bench runtime threads and raise sampling",
          "timestamp": "2026-08-28T00:03:23+02:00",
          "tree_id": "e70ac7f0b854adff8d7a19ee4fe80d46869ed424",
          "url": "https://github.com/hseeberger/tellus/commit/f7e8a3ec45a0f24d8c8bbc25a3a8f12714245fcf"
        },
        "date": 1787868414923,
        "tool": "cargo",
        "benches": [
          {
            "name": "flood/unbounded",
            "value": 11609576,
            "range": "± 120307",
            "unit": "ns/iter"
          },
          {
            "name": "flood/bounded",
            "value": 10615836,
            "range": "± 62633",
            "unit": "ns/iter"
          },
          {
            "name": "ping_pong/pairs/1",
            "value": 663926,
            "range": "± 9446",
            "unit": "ns/iter"
          },
          {
            "name": "ping_pong/pairs/4",
            "value": 792834,
            "range": "± 12230",
            "unit": "ns/iter"
          },
          {
            "name": "fan_out/workers/4",
            "value": 4928923,
            "range": "± 110032",
            "unit": "ns/iter"
          },
          {
            "name": "fan_out/workers/16",
            "value": 3447873,
            "range": "± 28993",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "git@heikoseeberger.de",
            "name": "Heiko Seeberger",
            "username": "hseeberger"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "9392dec4a4ce622e105058bcd1c0f7f1804c001e",
          "message": "Merge pull request #56 from hseeberger/ci/no-tag-trigger\n\nci: drop tag trigger from CI workflow",
          "timestamp": "2026-08-28T00:13:21+02:00",
          "tree_id": "e99834098d29fbda0b78763d4c6c5488c76f4cc0",
          "url": "https://github.com/hseeberger/tellus/commit/9392dec4a4ce622e105058bcd1c0f7f1804c001e"
        },
        "date": 1787869010486,
        "tool": "cargo",
        "benches": [
          {
            "name": "flood/unbounded",
            "value": 12292762,
            "range": "± 243687",
            "unit": "ns/iter"
          },
          {
            "name": "flood/bounded",
            "value": 11739486,
            "range": "± 146448",
            "unit": "ns/iter"
          },
          {
            "name": "ping_pong/pairs/1",
            "value": 713348,
            "range": "± 8979",
            "unit": "ns/iter"
          },
          {
            "name": "ping_pong/pairs/4",
            "value": 801746,
            "range": "± 4419",
            "unit": "ns/iter"
          },
          {
            "name": "fan_out/workers/4",
            "value": 5685341,
            "range": "± 172098",
            "unit": "ns/iter"
          },
          {
            "name": "fan_out/workers/16",
            "value": 3576901,
            "range": "± 21364",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "git@heikoseeberger.de",
            "name": "Heiko Seeberger",
            "username": "hseeberger"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "459ca7503272e953ef66a61aa0f47d6732a6b42c",
          "message": "Merge pull request #59 from hseeberger/ci/informational-bench-gate\n\nci: make bench comparison informational",
          "timestamp": "2026-08-28T11:03:13+02:00",
          "tree_id": "dcc0235bc8c9ae9325d2f84a4d24a06a0b774da1",
          "url": "https://github.com/hseeberger/tellus/commit/459ca7503272e953ef66a61aa0f47d6732a6b42c"
        },
        "date": 1787908001228,
        "tool": "cargo",
        "benches": [
          {
            "name": "flood/unbounded",
            "value": 12188311,
            "range": "± 237619",
            "unit": "ns/iter"
          },
          {
            "name": "flood/bounded",
            "value": 12485920,
            "range": "± 243509",
            "unit": "ns/iter"
          },
          {
            "name": "ping_pong/pairs/1",
            "value": 676289,
            "range": "± 18882",
            "unit": "ns/iter"
          },
          {
            "name": "ping_pong/pairs/4",
            "value": 795321,
            "range": "± 4502",
            "unit": "ns/iter"
          },
          {
            "name": "fan_out/workers/4",
            "value": 4773845,
            "range": "± 91754",
            "unit": "ns/iter"
          },
          {
            "name": "fan_out/workers/16",
            "value": 3398305,
            "range": "± 19597",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "git@heikoseeberger.de",
            "name": "Heiko Seeberger",
            "username": "hseeberger"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "000d5bb542ddea72395185a6e662ebe2e136c94d",
          "message": "Merge pull request #61 from hseeberger/refactor/reduce-nesting-and-duplication\n\nrefactor: cut nesting and duplication in core and persistence",
          "timestamp": "2026-08-28T11:53:48+02:00",
          "tree_id": "b661c51c56c02fe96cb8f02b1e17b1389c161b2a",
          "url": "https://github.com/hseeberger/tellus/commit/000d5bb542ddea72395185a6e662ebe2e136c94d"
        },
        "date": 1787911029980,
        "tool": "cargo",
        "benches": [
          {
            "name": "flood/unbounded",
            "value": 13370823,
            "range": "± 369634",
            "unit": "ns/iter"
          },
          {
            "name": "flood/bounded",
            "value": 12477494,
            "range": "± 30304",
            "unit": "ns/iter"
          },
          {
            "name": "ping_pong/pairs/1",
            "value": 684312,
            "range": "± 14583",
            "unit": "ns/iter"
          },
          {
            "name": "ping_pong/pairs/4",
            "value": 787640,
            "range": "± 3577",
            "unit": "ns/iter"
          },
          {
            "name": "fan_out/workers/4",
            "value": 4945491,
            "range": "± 72036",
            "unit": "ns/iter"
          },
          {
            "name": "fan_out/workers/16",
            "value": 3414922,
            "range": "± 19803",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "git@heikoseeberger.de",
            "name": "Heiko Seeberger",
            "username": "hseeberger"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "92fa3b15bc5d904aab4a8d95a1b7171117f55582",
          "message": "Merge pull request #62 from hseeberger/refactor/harden-core-and-persistence\n\nrefactor: harden core and persistence guarantees",
          "timestamp": "2026-08-29T23:01:57+02:00",
          "tree_id": "91b0982e85d89aefa13f11991c36e87b39fe737d",
          "url": "https://github.com/hseeberger/tellus/commit/92fa3b15bc5d904aab4a8d95a1b7171117f55582"
        },
        "date": 1788037530728,
        "tool": "cargo",
        "benches": [
          {
            "name": "flood/unbounded",
            "value": 11977959,
            "range": "± 435282",
            "unit": "ns/iter"
          },
          {
            "name": "flood/bounded",
            "value": 16301949,
            "range": "± 391520",
            "unit": "ns/iter"
          },
          {
            "name": "ping_pong/pairs/1",
            "value": 675512,
            "range": "± 18379",
            "unit": "ns/iter"
          },
          {
            "name": "ping_pong/pairs/4",
            "value": 798563,
            "range": "± 5146",
            "unit": "ns/iter"
          },
          {
            "name": "fan_out/workers/4",
            "value": 4894184,
            "range": "± 58497",
            "unit": "ns/iter"
          },
          {
            "name": "fan_out/workers/16",
            "value": 3474352,
            "range": "± 26527",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "git@heikoseeberger.de",
            "name": "Heiko Seeberger",
            "username": "hseeberger"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "8b55230f8f5224e73fdd7e316b20a4ef3807f4ab",
          "message": "Merge pull request #63 from hseeberger/fix/backoff-recovery-bench\n\nfix: harden backoff config, recovery drop and bench sizing",
          "timestamp": "2026-08-30T10:27:24+02:00",
          "tree_id": "752ffdbbab3300c992bee0982ffe325d4e4010c1",
          "url": "https://github.com/hseeberger/tellus/commit/8b55230f8f5224e73fdd7e316b20a4ef3807f4ab"
        },
        "date": 1788078647353,
        "tool": "cargo",
        "benches": [
          {
            "name": "flood/unbounded",
            "value": 8990546,
            "range": "± 97135",
            "unit": "ns/iter"
          },
          {
            "name": "flood/bounded",
            "value": 9533760,
            "range": "± 101725",
            "unit": "ns/iter"
          },
          {
            "name": "ping_pong/pairs/1",
            "value": 518439,
            "range": "± 7084",
            "unit": "ns/iter"
          },
          {
            "name": "ping_pong/pairs/4",
            "value": 607607,
            "range": "± 4488",
            "unit": "ns/iter"
          },
          {
            "name": "fan_out/workers/4",
            "value": 4053910,
            "range": "± 70016",
            "unit": "ns/iter"
          },
          {
            "name": "fan_out/workers/16",
            "value": 2797214,
            "range": "± 13183",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "git@heikoseeberger.de",
            "name": "Heiko Seeberger",
            "username": "hseeberger"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "c8ffcccd00d6cb31fb101ac0b2dac89ac987ca5a",
          "message": "Merge pull request #60 from hseeberger/release-plz-2026-08-28T09-04-03Z\n\nchore: release",
          "timestamp": "2026-08-31T12:27:04+02:00",
          "tree_id": "cfef77b6fcf556c46845948c805b148e6f3b12a3",
          "url": "https://github.com/hseeberger/tellus/commit/c8ffcccd00d6cb31fb101ac0b2dac89ac987ca5a"
        },
        "date": 1788172234183,
        "tool": "cargo",
        "benches": [
          {
            "name": "flood/unbounded",
            "value": 12117258,
            "range": "± 365505",
            "unit": "ns/iter"
          },
          {
            "name": "flood/bounded",
            "value": 14944553,
            "range": "± 133686",
            "unit": "ns/iter"
          },
          {
            "name": "ping_pong/pairs/1",
            "value": 683597,
            "range": "± 18280",
            "unit": "ns/iter"
          },
          {
            "name": "ping_pong/pairs/4",
            "value": 811899,
            "range": "± 5058",
            "unit": "ns/iter"
          },
          {
            "name": "fan_out/workers/4",
            "value": 4800216,
            "range": "± 69641",
            "unit": "ns/iter"
          },
          {
            "name": "fan_out/workers/16",
            "value": 3432772,
            "range": "± 19410",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "git@heikoseeberger.de",
            "name": "Heiko Seeberger",
            "username": "hseeberger"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "0745187b2fffc1e5e98f51a5fbc429d538044cfe",
          "message": "Merge pull request #64 from hseeberger/dependabot/cargo/cargo-major-a925caaff2\n\nbuild(deps): bump hotpath from 0.23.3 to 0.24.0 in the cargo-major group across 1 directory",
          "timestamp": "2026-09-01T21:39:40+02:00",
          "tree_id": "6a6692047a3bd9542ec6452d5e1a2cf79b488561",
          "url": "https://github.com/hseeberger/tellus/commit/0745187b2fffc1e5e98f51a5fbc429d538044cfe"
        },
        "date": 1788291786067,
        "tool": "cargo",
        "benches": [
          {
            "name": "flood/unbounded",
            "value": 9063604,
            "range": "± 98133",
            "unit": "ns/iter"
          },
          {
            "name": "flood/bounded",
            "value": 11138218,
            "range": "± 69493",
            "unit": "ns/iter"
          },
          {
            "name": "ping_pong/pairs/1",
            "value": 537616,
            "range": "± 19527",
            "unit": "ns/iter"
          },
          {
            "name": "ping_pong/pairs/4",
            "value": 618187,
            "range": "± 2833",
            "unit": "ns/iter"
          },
          {
            "name": "fan_out/workers/4",
            "value": 4248150,
            "range": "± 125695",
            "unit": "ns/iter"
          },
          {
            "name": "fan_out/workers/16",
            "value": 2806349,
            "range": "± 13454",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "git@heikoseeberger.de",
            "name": "Heiko Seeberger",
            "username": "hseeberger"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "c008766f415ed9c4848b94b133c591e936ce451d",
          "message": "Merge pull request #67 from hseeberger/build/cargo-hack\n\nbuild: drive the check and lint feature matrix with cargo-hack",
          "timestamp": "2026-09-04T14:22:16+02:00",
          "tree_id": "f7c785cc5e1f96489bbbdd78fbc2b8cce6c595cc",
          "url": "https://github.com/hseeberger/tellus/commit/c008766f415ed9c4848b94b133c591e936ce451d"
        },
        "date": 1788524741445,
        "tool": "cargo",
        "benches": [
          {
            "name": "flood/unbounded",
            "value": 24497342,
            "range": "± 5161120",
            "unit": "ns/iter"
          },
          {
            "name": "flood/bounded",
            "value": 26205715,
            "range": "± 229481",
            "unit": "ns/iter"
          },
          {
            "name": "ping_pong/pairs/1",
            "value": 752062,
            "range": "± 31205",
            "unit": "ns/iter"
          },
          {
            "name": "ping_pong/pairs/4",
            "value": 842727,
            "range": "± 17235",
            "unit": "ns/iter"
          },
          {
            "name": "fan_out/workers/4",
            "value": 8080741,
            "range": "± 421711",
            "unit": "ns/iter"
          },
          {
            "name": "fan_out/workers/16",
            "value": 6438358,
            "range": "± 285346",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "git@heikoseeberger.de",
            "name": "Heiko Seeberger",
            "username": "hseeberger"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "f3996e8d5447cd1961ddcafa7881a7a1ea606af7",
          "message": "Merge pull request #68 from hseeberger/dependabot/cargo/cargo-minor-8d2c85db1c\n\nbuild(deps): bump uuid from 1.25.0 to 1.26.0 in the cargo-minor group",
          "timestamp": "2026-09-08T21:26:44+02:00",
          "tree_id": "11572b6facd660d429236b72c801d2fd9c4f8f16",
          "url": "https://github.com/hseeberger/tellus/commit/f3996e8d5447cd1961ddcafa7881a7a1ea606af7"
        },
        "date": 1788895843415,
        "tool": "cargo",
        "benches": [
          {
            "name": "flood/unbounded",
            "value": 13012880,
            "range": "± 115600",
            "unit": "ns/iter"
          },
          {
            "name": "flood/bounded",
            "value": 13110238,
            "range": "± 117212",
            "unit": "ns/iter"
          },
          {
            "name": "ping_pong/pairs/1",
            "value": 670659,
            "range": "± 26299",
            "unit": "ns/iter"
          },
          {
            "name": "ping_pong/pairs/4",
            "value": 822063,
            "range": "± 38224",
            "unit": "ns/iter"
          },
          {
            "name": "fan_out/workers/4",
            "value": 4864501,
            "range": "± 83511",
            "unit": "ns/iter"
          },
          {
            "name": "fan_out/workers/16",
            "value": 3437347,
            "range": "± 27174",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "git@heikoseeberger.de",
            "name": "Heiko Seeberger",
            "username": "hseeberger"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "777d77585d3af2440b768f780df2466e2019e8e6",
          "message": "Merge pull request #69 from hseeberger/dependabot/github_actions/ci-minor-fcbeee0005\n\nci(deps): bump taiki-e/install-action from 2.86.7 to 2.87.3 in the ci-minor group",
          "timestamp": "2026-09-08T21:32:03+02:00",
          "tree_id": "da673ef3816b3932f0b189fc920af9ce8c0a2f8b",
          "url": "https://github.com/hseeberger/tellus/commit/777d77585d3af2440b768f780df2466e2019e8e6"
        },
        "date": 1788896126764,
        "tool": "cargo",
        "benches": [
          {
            "name": "flood/unbounded",
            "value": 11505781,
            "range": "± 110854",
            "unit": "ns/iter"
          },
          {
            "name": "flood/bounded",
            "value": 12177955,
            "range": "± 200649",
            "unit": "ns/iter"
          },
          {
            "name": "ping_pong/pairs/1",
            "value": 648428,
            "range": "± 27637",
            "unit": "ns/iter"
          },
          {
            "name": "ping_pong/pairs/4",
            "value": 791461,
            "range": "± 20026",
            "unit": "ns/iter"
          },
          {
            "name": "fan_out/workers/4",
            "value": 4911207,
            "range": "± 184546",
            "unit": "ns/iter"
          },
          {
            "name": "fan_out/workers/16",
            "value": 3417050,
            "range": "± 18659",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "git@heikoseeberger.de",
            "name": "Heiko Seeberger",
            "username": "hseeberger"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "521e3495b152643addd15034394faf9ba8215200",
          "message": "Merge pull request #70 from hseeberger/build/rust\n\nbuild(deps): bump Rust to 1.98.1",
          "timestamp": "2026-09-10T15:07:59+03:00",
          "tree_id": "754e22e20c6c5fc17d3cfd2920c596b3f8665b40",
          "url": "https://github.com/hseeberger/tellus/commit/521e3495b152643addd15034394faf9ba8215200"
        },
        "date": 1789042312724,
        "tool": "cargo",
        "benches": [
          {
            "name": "flood/unbounded",
            "value": 9188026,
            "range": "± 248445",
            "unit": "ns/iter"
          },
          {
            "name": "flood/bounded",
            "value": 8219258,
            "range": "± 24959",
            "unit": "ns/iter"
          },
          {
            "name": "ping_pong/pairs/1",
            "value": 523244,
            "range": "± 4855",
            "unit": "ns/iter"
          },
          {
            "name": "ping_pong/pairs/4",
            "value": 599991,
            "range": "± 2789",
            "unit": "ns/iter"
          },
          {
            "name": "fan_out/workers/4",
            "value": 4295769,
            "range": "± 104682",
            "unit": "ns/iter"
          },
          {
            "name": "fan_out/workers/16",
            "value": 2770684,
            "range": "± 9007",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "git@heikoseeberger.de",
            "name": "Heiko Seeberger",
            "username": "hseeberger"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "f0a42030e28cbc1a66090cefda4d1c2b836939c2",
          "message": "Merge pull request #72 from hseeberger/feat/actor-system-stop\n\nfeat: stop an actor system from the outside",
          "timestamp": "2026-09-13T11:03:13+03:00",
          "tree_id": "0489674a649d1aac43dfc1308fc89f3b6e35bf9e",
          "url": "https://github.com/hseeberger/tellus/commit/f0a42030e28cbc1a66090cefda4d1c2b836939c2"
        },
        "date": 1789286844349,
        "tool": "cargo",
        "benches": [
          {
            "name": "flood/unbounded",
            "value": 17816938,
            "range": "± 144376",
            "unit": "ns/iter"
          },
          {
            "name": "flood/bounded",
            "value": 11734355,
            "range": "± 103491",
            "unit": "ns/iter"
          },
          {
            "name": "ping_pong/pairs/1",
            "value": 703182,
            "range": "± 21343",
            "unit": "ns/iter"
          },
          {
            "name": "ping_pong/pairs/4",
            "value": 800288,
            "range": "± 3961",
            "unit": "ns/iter"
          },
          {
            "name": "fan_out/workers/4",
            "value": 4942234,
            "range": "± 140854",
            "unit": "ns/iter"
          },
          {
            "name": "fan_out/workers/16",
            "value": 3511143,
            "range": "± 11131",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "git@heikoseeberger.de",
            "name": "Heiko Seeberger",
            "username": "hseeberger"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "48ce2a29421baad394de223b318f6d27e3b9ff14",
          "message": "Merge pull request #74 from hseeberger/feat/in-memory-store\n\nfeat: add in-memory store behind persistence-in-memory feature",
          "timestamp": "2026-09-13T17:58:37+03:00",
          "tree_id": "c35b0d128021ae17191aeb230676e9864508537f",
          "url": "https://github.com/hseeberger/tellus/commit/48ce2a29421baad394de223b318f6d27e3b9ff14"
        },
        "date": 1789311721330,
        "tool": "cargo",
        "benches": [
          {
            "name": "flood/unbounded",
            "value": 11298966,
            "range": "± 153192",
            "unit": "ns/iter"
          },
          {
            "name": "flood/bounded",
            "value": 16053833,
            "range": "± 1058930",
            "unit": "ns/iter"
          },
          {
            "name": "ping_pong/pairs/1",
            "value": 699232,
            "range": "± 17214",
            "unit": "ns/iter"
          },
          {
            "name": "ping_pong/pairs/4",
            "value": 799919,
            "range": "± 6446",
            "unit": "ns/iter"
          },
          {
            "name": "fan_out/workers/4",
            "value": 4994275,
            "range": "± 62038",
            "unit": "ns/iter"
          },
          {
            "name": "fan_out/workers/16",
            "value": 3565174,
            "range": "± 97620",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "git@heikoseeberger.de",
            "name": "Heiko Seeberger",
            "username": "hseeberger"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "48e01ffd17482451cf6b59345b380eb0c6084932",
          "message": "Merge pull request #73 from hseeberger/feat/host\n\nfeat: add hosts, spawning/passivating managed entities",
          "timestamp": "2026-09-13T18:54:26+03:00",
          "tree_id": "f16b3b187a342ef5f7a4c11d6e20d9f65225237b",
          "url": "https://github.com/hseeberger/tellus/commit/48e01ffd17482451cf6b59345b380eb0c6084932"
        },
        "date": 1789315073177,
        "tool": "cargo",
        "benches": [
          {
            "name": "flood/unbounded",
            "value": 10975437,
            "range": "± 270249",
            "unit": "ns/iter"
          },
          {
            "name": "flood/bounded",
            "value": 11207145,
            "range": "± 97759",
            "unit": "ns/iter"
          },
          {
            "name": "ping_pong/pairs/1",
            "value": 680548,
            "range": "± 17164",
            "unit": "ns/iter"
          },
          {
            "name": "ping_pong/pairs/4",
            "value": 792715,
            "range": "± 4901",
            "unit": "ns/iter"
          },
          {
            "name": "fan_out/workers/4",
            "value": 4976631,
            "range": "± 96905",
            "unit": "ns/iter"
          },
          {
            "name": "fan_out/workers/16",
            "value": 3473937,
            "range": "± 54229",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "git@heikoseeberger.de",
            "name": "Heiko Seeberger",
            "username": "hseeberger"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "4b988a401be5cdffb487c0cf1abffaf8d15d7970",
          "message": "Merge pull request #76 from hseeberger/release-plz-2026-09-13T15-55-22Z\n\nchore: release",
          "timestamp": "2026-09-13T19:22:05+03:00",
          "tree_id": "103de6f97c2fe22571dfd29cf6d126d276770610",
          "url": "https://github.com/hseeberger/tellus/commit/4b988a401be5cdffb487c0cf1abffaf8d15d7970"
        },
        "date": 1789316729216,
        "tool": "cargo",
        "benches": [
          {
            "name": "flood/unbounded",
            "value": 11811816,
            "range": "± 714104",
            "unit": "ns/iter"
          },
          {
            "name": "flood/bounded",
            "value": 11242712,
            "range": "± 1182451",
            "unit": "ns/iter"
          },
          {
            "name": "ping_pong/pairs/1",
            "value": 660146,
            "range": "± 17783",
            "unit": "ns/iter"
          },
          {
            "name": "ping_pong/pairs/4",
            "value": 793560,
            "range": "± 10494",
            "unit": "ns/iter"
          },
          {
            "name": "fan_out/workers/4",
            "value": 5123820,
            "range": "± 65305",
            "unit": "ns/iter"
          },
          {
            "name": "fan_out/workers/16",
            "value": 3515902,
            "range": "± 28519",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "git@heikoseeberger.de",
            "name": "Heiko Seeberger",
            "username": "hseeberger"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "9a7e5020681c36622c2277dca02f50970f4ad0a4",
          "message": "Merge pull request #77 from hseeberger/test/faulty-store\n\ntest: replace TestStore with FaultyStore over InMemoryStore",
          "timestamp": "2026-09-13T22:01:09+03:00",
          "tree_id": "5625602f3c93c03e3e2c5c5266686129f81c94a9",
          "url": "https://github.com/hseeberger/tellus/commit/9a7e5020681c36622c2277dca02f50970f4ad0a4"
        },
        "date": 1789326283003,
        "tool": "cargo",
        "benches": [
          {
            "name": "flood/unbounded",
            "value": 20837455,
            "range": "± 785298",
            "unit": "ns/iter"
          },
          {
            "name": "flood/bounded",
            "value": 21777304,
            "range": "± 2735991",
            "unit": "ns/iter"
          },
          {
            "name": "ping_pong/pairs/1",
            "value": 811033,
            "range": "± 13070",
            "unit": "ns/iter"
          },
          {
            "name": "ping_pong/pairs/4",
            "value": 931265,
            "range": "± 14550",
            "unit": "ns/iter"
          },
          {
            "name": "fan_out/workers/4",
            "value": 8470504,
            "range": "± 467196",
            "unit": "ns/iter"
          },
          {
            "name": "fan_out/workers/16",
            "value": 8695396,
            "range": "± 400884",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "git@heikoseeberger.de",
            "name": "Heiko Seeberger",
            "username": "hseeberger"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "d1e9362f7359ad8d474fd888e6b5df3ebb3c12db",
          "message": "Merge pull request #80 from hseeberger/dependabot/cargo/cargo-major-dc3657b4df\n\nbuild(deps): bump hotpath from 0.24.0 to 0.25.1 in the cargo-major group",
          "timestamp": "2026-09-15T21:37:26+03:00",
          "tree_id": "449df767d4c6166fe24893b783ad3fd3ab0b5d32",
          "url": "https://github.com/hseeberger/tellus/commit/d1e9362f7359ad8d474fd888e6b5df3ebb3c12db"
        },
        "date": 1789497655289,
        "tool": "cargo",
        "benches": [
          {
            "name": "flood/unbounded",
            "value": 31776200,
            "range": "± 3493830",
            "unit": "ns/iter"
          },
          {
            "name": "flood/bounded",
            "value": 35569447,
            "range": "± 687183",
            "unit": "ns/iter"
          },
          {
            "name": "ping_pong/pairs/1",
            "value": 710059,
            "range": "± 21267",
            "unit": "ns/iter"
          },
          {
            "name": "ping_pong/pairs/4",
            "value": 835066,
            "range": "± 64116",
            "unit": "ns/iter"
          },
          {
            "name": "fan_out/workers/4",
            "value": 8235262,
            "range": "± 274996",
            "unit": "ns/iter"
          },
          {
            "name": "fan_out/workers/16",
            "value": 6255824,
            "range": "± 334340",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "git@heikoseeberger.de",
            "name": "Heiko Seeberger",
            "username": "hseeberger"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "907e5dd18f73d8981cbfa249563e842a80d321ec",
          "message": "Merge pull request #84 from hseeberger/fix/hotpath-utils-version\n\nci: install hotpath-utils matching the hotpath version",
          "timestamp": "2026-09-16T09:39:34+03:00",
          "tree_id": "2542b23d7ffac9d852bf76ed17599bb220c982dd",
          "url": "https://github.com/hseeberger/tellus/commit/907e5dd18f73d8981cbfa249563e842a80d321ec"
        },
        "date": 1789540976308,
        "tool": "cargo",
        "benches": [
          {
            "name": "flood/unbounded",
            "value": 11856554,
            "range": "± 62118",
            "unit": "ns/iter"
          },
          {
            "name": "flood/bounded",
            "value": 13464431,
            "range": "± 106257",
            "unit": "ns/iter"
          },
          {
            "name": "ping_pong/pairs/1",
            "value": 722342,
            "range": "± 17907",
            "unit": "ns/iter"
          },
          {
            "name": "ping_pong/pairs/4",
            "value": 824369,
            "range": "± 17001",
            "unit": "ns/iter"
          },
          {
            "name": "fan_out/workers/4",
            "value": 5184277,
            "range": "± 89741",
            "unit": "ns/iter"
          },
          {
            "name": "fan_out/workers/16",
            "value": 3513012,
            "range": "± 16047",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "git@heikoseeberger.de",
            "name": "Heiko Seeberger",
            "username": "hseeberger"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "bf073f6f16b533145ebc9e0a22f7256356857268",
          "message": "Merge pull request #83 from hseeberger/fix/79-rustsec-2026-0285\n\nfix(deps): bump rustls to 0.23.45 for RUSTSEC-2026-0285",
          "timestamp": "2026-09-16T09:46:58+03:00",
          "tree_id": "1da2d3fe3bb0592c7c23fd2a904116819332babc",
          "url": "https://github.com/hseeberger/tellus/commit/bf073f6f16b533145ebc9e0a22f7256356857268"
        },
        "date": 1789541428813,
        "tool": "cargo",
        "benches": [
          {
            "name": "flood/unbounded",
            "value": 12670897,
            "range": "± 223584",
            "unit": "ns/iter"
          },
          {
            "name": "flood/bounded",
            "value": 15611636,
            "range": "± 343278",
            "unit": "ns/iter"
          },
          {
            "name": "ping_pong/pairs/1",
            "value": 653225,
            "range": "± 14920",
            "unit": "ns/iter"
          },
          {
            "name": "ping_pong/pairs/4",
            "value": 793025,
            "range": "± 4615",
            "unit": "ns/iter"
          },
          {
            "name": "fan_out/workers/4",
            "value": 5197569,
            "range": "± 99912",
            "unit": "ns/iter"
          },
          {
            "name": "fan_out/workers/16",
            "value": 3510170,
            "range": "± 20855",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "git@heikoseeberger.de",
            "name": "Heiko Seeberger",
            "username": "hseeberger"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "108c7b45ee230b0d3d8a6cd667a887c743f5e95c",
          "message": "Merge pull request #89 from hseeberger/dependabot/github_actions/dtolnay/rust-toolchain-02cb101ec7c40f2c49e1d9714d64511d8e1b74de\n\nci(deps): bump dtolnay/rust-toolchain from 6c977a6ca4077a0ceb28ffbe03f59d46e9ac8772 to 02cb101ec7c40f2c49e1d9714d64511d8e1b74de",
          "timestamp": "2026-09-23T00:22:23+03:00",
          "tree_id": "6acc43375531f2cabd3250fa2af30f4f80fa6790",
          "url": "https://github.com/hseeberger/tellus/commit/108c7b45ee230b0d3d8a6cd667a887c743f5e95c"
        },
        "date": 1790112352633,
        "tool": "cargo",
        "benches": [
          {
            "name": "flood/unbounded",
            "value": 13919617,
            "range": "± 301605",
            "unit": "ns/iter"
          },
          {
            "name": "flood/bounded",
            "value": 17013970,
            "range": "± 165679",
            "unit": "ns/iter"
          },
          {
            "name": "ping_pong/pairs/1",
            "value": 711582,
            "range": "± 19682",
            "unit": "ns/iter"
          },
          {
            "name": "ping_pong/pairs/4",
            "value": 829436,
            "range": "± 11560",
            "unit": "ns/iter"
          },
          {
            "name": "fan_out/workers/4",
            "value": 5209798,
            "range": "± 92822",
            "unit": "ns/iter"
          },
          {
            "name": "fan_out/workers/16",
            "value": 3564933,
            "range": "± 40158",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "git@heikoseeberger.de",
            "name": "Heiko Seeberger",
            "username": "hseeberger"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "fe53a3210d3e3f716b430a974880a2bc0ffb3659",
          "message": "Merge pull request #91 from hseeberger/dependabot/cargo/cargo-major-f1df897d83\n\nbuild(deps): bump hotpath from 0.25.1 to 0.26.0 in the cargo-major group",
          "timestamp": "2026-09-30T11:10:34+02:00",
          "tree_id": "24296d02809b554a7ae016d2020c7ffe8d25763f",
          "url": "https://github.com/hseeberger/tellus/commit/fe53a3210d3e3f716b430a974880a2bc0ffb3659"
        },
        "date": 1790759655545,
        "tool": "cargo",
        "benches": [
          {
            "name": "flood/unbounded",
            "value": 12688514,
            "range": "± 107300",
            "unit": "ns/iter"
          },
          {
            "name": "flood/bounded",
            "value": 14257258,
            "range": "± 116051",
            "unit": "ns/iter"
          },
          {
            "name": "ping_pong/pairs/1",
            "value": 644973,
            "range": "± 13774",
            "unit": "ns/iter"
          },
          {
            "name": "ping_pong/pairs/4",
            "value": 803657,
            "range": "± 9487",
            "unit": "ns/iter"
          },
          {
            "name": "fan_out/workers/4",
            "value": 4904446,
            "range": "± 85628",
            "unit": "ns/iter"
          },
          {
            "name": "fan_out/workers/16",
            "value": 3473486,
            "range": "± 11315",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "git@heikoseeberger.de",
            "name": "Heiko Seeberger",
            "username": "hseeberger"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "572c0f2100953f753c69a94824f7c988cb3f232a",
          "message": "Merge pull request #98 from hseeberger/dependabot/github_actions/dtolnay/rust-toolchain-7e38f4b43b4db5c8dd498af069a4f6196df1d067\n\nci(deps): bump dtolnay/rust-toolchain from 02cb101ec7c40f2c49e1d9714d64511d8e1b74de to 7e38f4b43b4db5c8dd498af069a4f6196df1d067",
          "timestamp": "2026-10-08T11:43:19+02:00",
          "tree_id": "61c0b543eee8c8666a198596fd3919c159d68509",
          "url": "https://github.com/hseeberger/tellus/commit/572c0f2100953f753c69a94824f7c988cb3f232a"
        },
        "date": 1791452834794,
        "tool": "cargo",
        "benches": [
          {
            "name": "flood/unbounded",
            "value": 9911078,
            "range": "± 210795",
            "unit": "ns/iter"
          },
          {
            "name": "flood/bounded",
            "value": 10182694,
            "range": "± 539693",
            "unit": "ns/iter"
          },
          {
            "name": "ping_pong/pairs/1",
            "value": 540907,
            "range": "± 9230",
            "unit": "ns/iter"
          },
          {
            "name": "ping_pong/pairs/4",
            "value": 606163,
            "range": "± 9771",
            "unit": "ns/iter"
          },
          {
            "name": "fan_out/workers/4",
            "value": 4314459,
            "range": "± 129023",
            "unit": "ns/iter"
          },
          {
            "name": "fan_out/workers/16",
            "value": 2832292,
            "range": "± 21039",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "git@heikoseeberger.de",
            "name": "Heiko Seeberger",
            "username": "hseeberger"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "1b0d058ddd53a1d1b41287f8c867fe32e74d1352",
          "message": "Merge pull request #96 from hseeberger/dependabot/cargo/cargo-major-4e228f18ff\n\nbuild(deps): bump hotpath from 0.26.1 to 0.28.0 in the cargo-major group across 1 directory",
          "timestamp": "2026-10-08T11:52:16+02:00",
          "tree_id": "ea605ca56da5c5c004141fb6171c5dd9e303b20e",
          "url": "https://github.com/hseeberger/tellus/commit/1b0d058ddd53a1d1b41287f8c867fe32e74d1352"
        },
        "date": 1791453359109,
        "tool": "cargo",
        "benches": [
          {
            "name": "flood/unbounded",
            "value": 9810822,
            "range": "± 451612",
            "unit": "ns/iter"
          },
          {
            "name": "flood/bounded",
            "value": 12238076,
            "range": "± 609304",
            "unit": "ns/iter"
          },
          {
            "name": "ping_pong/pairs/1",
            "value": 657595,
            "range": "± 36623",
            "unit": "ns/iter"
          },
          {
            "name": "ping_pong/pairs/4",
            "value": 560117,
            "range": "± 4383",
            "unit": "ns/iter"
          },
          {
            "name": "fan_out/workers/4",
            "value": 4053921,
            "range": "± 83920",
            "unit": "ns/iter"
          },
          {
            "name": "fan_out/workers/16",
            "value": 2593329,
            "range": "± 62262",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "git@heikoseeberger.de",
            "name": "Heiko Seeberger",
            "username": "hseeberger"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "fada3f8a503ccf0486b49e9165ff8d1982694a3a",
          "message": "Merge pull request #99 from pawurb/hotpath-cloud\n\nci: migrate profiling to hotpath cloud",
          "timestamp": "2026-10-10T16:59:06+02:00",
          "tree_id": "c7de65dfa3ef506f0adb63107b1596577e43561e",
          "url": "https://github.com/hseeberger/tellus/commit/fada3f8a503ccf0486b49e9165ff8d1982694a3a"
        },
        "date": 1791644549607,
        "tool": "cargo",
        "benches": [
          {
            "name": "flood/unbounded",
            "value": 11964586,
            "range": "± 216399",
            "unit": "ns/iter"
          },
          {
            "name": "flood/bounded",
            "value": 12673860,
            "range": "± 36544",
            "unit": "ns/iter"
          },
          {
            "name": "ping_pong/pairs/1",
            "value": 672148,
            "range": "± 17120",
            "unit": "ns/iter"
          },
          {
            "name": "ping_pong/pairs/4",
            "value": 786643,
            "range": "± 4421",
            "unit": "ns/iter"
          },
          {
            "name": "fan_out/workers/4",
            "value": 4953359,
            "range": "± 83454",
            "unit": "ns/iter"
          },
          {
            "name": "fan_out/workers/16",
            "value": 3450285,
            "range": "± 19643",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "git@heikoseeberger.de",
            "name": "Heiko Seeberger",
            "username": "hseeberger"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "4c1ea36554936b83207a6697b050f692aa4c21ee",
          "message": "Merge pull request #104 from hseeberger/ci/drop-alloc-gate-retry\n\nci: drop the zero-alloc gate retry now that hotpath is fixed",
          "timestamp": "2026-10-10T19:03:17+02:00",
          "tree_id": "0dfd00729ceb4b54c4a4622819a80a0c6e7bc8f1",
          "url": "https://github.com/hseeberger/tellus/commit/4c1ea36554936b83207a6697b050f692aa4c21ee"
        },
        "date": 1791652001911,
        "tool": "cargo",
        "benches": [
          {
            "name": "flood/unbounded",
            "value": 9751693,
            "range": "± 402658",
            "unit": "ns/iter"
          },
          {
            "name": "flood/bounded",
            "value": 11571481,
            "range": "± 849207",
            "unit": "ns/iter"
          },
          {
            "name": "ping_pong/pairs/1",
            "value": 696906,
            "range": "± 24416",
            "unit": "ns/iter"
          },
          {
            "name": "ping_pong/pairs/4",
            "value": 573877,
            "range": "± 9654",
            "unit": "ns/iter"
          },
          {
            "name": "fan_out/workers/4",
            "value": 4156677,
            "range": "± 96682",
            "unit": "ns/iter"
          },
          {
            "name": "fan_out/workers/16",
            "value": 2990885,
            "range": "± 104831",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "git@heikoseeberger.de",
            "name": "Heiko Seeberger",
            "username": "hseeberger"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "1cfc9e5d3904714cc4b7667ad8f631f459931d47",
          "message": "Merge pull request #103 from hseeberger/feat/test-context\n\nfeat: add TestContext behind test-util for unit testing actors",
          "timestamp": "2026-10-12T16:47:14+02:00",
          "tree_id": "7d0bdb13ad3b51c609a683d75f11e905f8b4b925",
          "url": "https://github.com/hseeberger/tellus/commit/1cfc9e5d3904714cc4b7667ad8f631f459931d47"
        },
        "date": 1791816645293,
        "tool": "cargo",
        "benches": [
          {
            "name": "flood/unbounded",
            "value": 10033087,
            "range": "± 293366",
            "unit": "ns/iter"
          },
          {
            "name": "flood/bounded",
            "value": 10318693,
            "range": "± 52643",
            "unit": "ns/iter"
          },
          {
            "name": "ping_pong/pairs/1",
            "value": 537456,
            "range": "± 6019",
            "unit": "ns/iter"
          },
          {
            "name": "ping_pong/pairs/4",
            "value": 611347,
            "range": "± 3733",
            "unit": "ns/iter"
          },
          {
            "name": "fan_out/workers/4",
            "value": 4446265,
            "range": "± 168940",
            "unit": "ns/iter"
          },
          {
            "name": "fan_out/workers/16",
            "value": 2819489,
            "range": "± 16995",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "git@heikoseeberger.de",
            "name": "Heiko Seeberger",
            "username": "hseeberger"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "5faf50446f320012f5722a64c7f6749c921301a7",
          "message": "Merge pull request #107 from hseeberger/build/rust\n\nbuild(deps): bump Rust to 1.99.0",
          "timestamp": "2026-10-12T17:43:57+02:00",
          "tree_id": "34fc20f51fe0c6eb87bc54579677687115d2c437",
          "url": "https://github.com/hseeberger/tellus/commit/5faf50446f320012f5722a64c7f6749c921301a7"
        },
        "date": 1791820089055,
        "tool": "cargo",
        "benches": [
          {
            "name": "flood/unbounded",
            "value": 23599255,
            "range": "± 2717317",
            "unit": "ns/iter"
          },
          {
            "name": "flood/bounded",
            "value": 21263734,
            "range": "± 724426",
            "unit": "ns/iter"
          },
          {
            "name": "ping_pong/pairs/1",
            "value": 803448,
            "range": "± 11394",
            "unit": "ns/iter"
          },
          {
            "name": "ping_pong/pairs/4",
            "value": 935270,
            "range": "± 14793",
            "unit": "ns/iter"
          },
          {
            "name": "fan_out/workers/4",
            "value": 9292842,
            "range": "± 687223",
            "unit": "ns/iter"
          },
          {
            "name": "fan_out/workers/16",
            "value": 8706546,
            "range": "± 335110",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "git@heikoseeberger.de",
            "name": "Heiko Seeberger",
            "username": "hseeberger"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "75be86ade0a786c469a7520ab70738daa75a729e",
          "message": "Merge pull request #106 from hseeberger/release-plz-2026-10-12T14-48-32Z\n\nchore: release",
          "timestamp": "2026-10-12T19:05:54+02:00",
          "tree_id": "0a9d57698312bfa4a661f85dfb135dd82e6eeba4",
          "url": "https://github.com/hseeberger/tellus/commit/75be86ade0a786c469a7520ab70738daa75a729e"
        },
        "date": 1791824953003,
        "tool": "cargo",
        "benches": [
          {
            "name": "flood/unbounded",
            "value": 25546474,
            "range": "± 4754541",
            "unit": "ns/iter"
          },
          {
            "name": "flood/bounded",
            "value": 38666185,
            "range": "± 5775169",
            "unit": "ns/iter"
          },
          {
            "name": "ping_pong/pairs/1",
            "value": 642490,
            "range": "± 9095",
            "unit": "ns/iter"
          },
          {
            "name": "ping_pong/pairs/4",
            "value": 785836,
            "range": "± 34807",
            "unit": "ns/iter"
          },
          {
            "name": "fan_out/workers/4",
            "value": 8237526,
            "range": "± 466852",
            "unit": "ns/iter"
          },
          {
            "name": "fan_out/workers/16",
            "value": 6512694,
            "range": "± 368270",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "git@heikoseeberger.de",
            "name": "Heiko Seeberger",
            "username": "hseeberger"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "bb08edf708fad42f2d674e754131eac1e307798d",
          "message": "Merge pull request #110 from hseeberger/dependabot/github_actions/dtolnay/rust-toolchain-e2a55d2ffb04f378e9626c28d38b36d230d1e12f\n\nci(deps): bump dtolnay/rust-toolchain from 7e38f4b43b4db5c8dd498af069a4f6196df1d067 to e2a55d2ffb04f378e9626c28d38b36d230d1e12f",
          "timestamp": "2026-10-14T21:13:48+02:00",
          "tree_id": "6bc9d01e9f9b1d89aa4c898f10b4ea3525bd26b6",
          "url": "https://github.com/hseeberger/tellus/commit/bb08edf708fad42f2d674e754131eac1e307798d"
        },
        "date": 1792005429377,
        "tool": "cargo",
        "benches": [
          {
            "name": "flood/unbounded",
            "value": 8210656,
            "range": "± 285600",
            "unit": "ns/iter"
          },
          {
            "name": "flood/bounded",
            "value": 10483906,
            "range": "± 264368",
            "unit": "ns/iter"
          },
          {
            "name": "ping_pong/pairs/1",
            "value": 591113,
            "range": "± 27536",
            "unit": "ns/iter"
          },
          {
            "name": "ping_pong/pairs/4",
            "value": 545226,
            "range": "± 10024",
            "unit": "ns/iter"
          },
          {
            "name": "fan_out/workers/4",
            "value": 3927290,
            "range": "± 63162",
            "unit": "ns/iter"
          },
          {
            "name": "fan_out/workers/16",
            "value": 2473152,
            "range": "± 29723",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "git@heikoseeberger.de",
            "name": "Heiko Seeberger",
            "username": "hseeberger"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "bbcc71797fd40924b72aff281237502abdf1f4c8",
          "message": "Merge pull request #109 from hseeberger/dependabot/cargo/cargo-minor-c15a9d44dc\n\nbuild(deps): bump uuid from 1.26.1 to 1.27.0 in the cargo-minor group across 1 directory",
          "timestamp": "2026-10-14T21:26:39+02:00",
          "tree_id": "f04e30fb4a7aba7b3495ed786dfb5c9fe9a8a4b8",
          "url": "https://github.com/hseeberger/tellus/commit/bbcc71797fd40924b72aff281237502abdf1f4c8"
        },
        "date": 1792006218615,
        "tool": "cargo",
        "benches": [
          {
            "name": "flood/unbounded",
            "value": 10259638,
            "range": "± 409823",
            "unit": "ns/iter"
          },
          {
            "name": "flood/bounded",
            "value": 11097988,
            "range": "± 318882",
            "unit": "ns/iter"
          },
          {
            "name": "ping_pong/pairs/1",
            "value": 510175,
            "range": "± 5599",
            "unit": "ns/iter"
          },
          {
            "name": "ping_pong/pairs/4",
            "value": 612829,
            "range": "± 4402",
            "unit": "ns/iter"
          },
          {
            "name": "fan_out/workers/4",
            "value": 4063309,
            "range": "± 177957",
            "unit": "ns/iter"
          },
          {
            "name": "fan_out/workers/16",
            "value": 2855361,
            "range": "± 22295",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "git@heikoseeberger.de",
            "name": "Heiko Seeberger",
            "username": "hseeberger"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "160f493ca931d0d3422eceac533435e911af065c",
          "message": "Merge pull request #116 from hseeberger/fix/rustsec-rustls\n\nfix(deps): bump rustls to 0.23.46",
          "timestamp": "2026-10-15T14:41:16+02:00",
          "tree_id": "cdae37d67396495d271a829fed3dd8375459c953",
          "url": "https://github.com/hseeberger/tellus/commit/160f493ca931d0d3422eceac533435e911af065c"
        },
        "date": 1792068282306,
        "tool": "cargo",
        "benches": [
          {
            "name": "flood/unbounded",
            "value": 11851354,
            "range": "± 289602",
            "unit": "ns/iter"
          },
          {
            "name": "flood/bounded",
            "value": 14322946,
            "range": "± 123780",
            "unit": "ns/iter"
          },
          {
            "name": "ping_pong/pairs/1",
            "value": 674735,
            "range": "± 19223",
            "unit": "ns/iter"
          },
          {
            "name": "ping_pong/pairs/4",
            "value": 787003,
            "range": "± 5119",
            "unit": "ns/iter"
          },
          {
            "name": "fan_out/workers/4",
            "value": 4761801,
            "range": "± 104507",
            "unit": "ns/iter"
          },
          {
            "name": "fan_out/workers/16",
            "value": 3518018,
            "range": "± 15477",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "git@heikoseeberger.de",
            "name": "Heiko Seeberger",
            "username": "hseeberger"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "2218e9ce5a93c76d726c4cd5182282019a6da812",
          "message": "Merge pull request #117 from hseeberger/chore/agents\n\nchore: use AGENTS.md instead of CLAUDE.md",
          "timestamp": "2026-10-16T19:06:55+02:00",
          "tree_id": "bd58a0b8c7c8279b2dc3f7d324c8b251ff0ba20e",
          "url": "https://github.com/hseeberger/tellus/commit/2218e9ce5a93c76d726c4cd5182282019a6da812"
        },
        "date": 1792170616032,
        "tool": "cargo",
        "benches": [
          {
            "name": "flood/unbounded",
            "value": 13245134,
            "range": "± 158380",
            "unit": "ns/iter"
          },
          {
            "name": "flood/bounded",
            "value": 12802377,
            "range": "± 287016",
            "unit": "ns/iter"
          },
          {
            "name": "ping_pong/pairs/1",
            "value": 655461,
            "range": "± 22603",
            "unit": "ns/iter"
          },
          {
            "name": "ping_pong/pairs/4",
            "value": 768895,
            "range": "± 16045",
            "unit": "ns/iter"
          },
          {
            "name": "fan_out/workers/4",
            "value": 5005747,
            "range": "± 141026",
            "unit": "ns/iter"
          },
          {
            "name": "fan_out/workers/16",
            "value": 3546650,
            "range": "± 140770",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "git@heikoseeberger.de",
            "name": "Heiko Seeberger",
            "username": "hseeberger"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "8fa5ed5606022b2e32e677ed201840298fbc8810",
          "message": "Merge pull request #118 from hseeberger/chore/drop-homepage\n\nchore: drop redundant homepage from manifests",
          "timestamp": "2026-10-17T09:18:28+02:00",
          "tree_id": "956a033568addc822a18a47e1bb5aba0080e3d9e",
          "url": "https://github.com/hseeberger/tellus/commit/8fa5ed5606022b2e32e677ed201840298fbc8810"
        },
        "date": 1792221726527,
        "tool": "cargo",
        "benches": [
          {
            "name": "flood/unbounded",
            "value": 26609705,
            "range": "± 1540253",
            "unit": "ns/iter"
          },
          {
            "name": "flood/bounded",
            "value": 29851631,
            "range": "± 3627606",
            "unit": "ns/iter"
          },
          {
            "name": "ping_pong/pairs/1",
            "value": 909139,
            "range": "± 6506",
            "unit": "ns/iter"
          },
          {
            "name": "ping_pong/pairs/4",
            "value": 1084542,
            "range": "± 14119",
            "unit": "ns/iter"
          },
          {
            "name": "fan_out/workers/4",
            "value": 10278517,
            "range": "± 299015",
            "unit": "ns/iter"
          },
          {
            "name": "fan_out/workers/16",
            "value": 9896803,
            "range": "± 380900",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "git@heikoseeberger.de",
            "name": "Heiko Seeberger",
            "username": "hseeberger"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "195710f2acbabf6493e757497db2dc926adfd611",
          "message": "Merge pull request #120 from hseeberger/chore/dev-profile-debug\n\nbuild: shrink dev build output via debug profile settings",
          "timestamp": "2026-10-17T17:43:49+02:00",
          "tree_id": "287fd2dcd81c3030276084043f6d2b725ac7ae4b",
          "url": "https://github.com/hseeberger/tellus/commit/195710f2acbabf6493e757497db2dc926adfd611"
        },
        "date": 1792252029232,
        "tool": "cargo",
        "benches": [
          {
            "name": "flood/unbounded",
            "value": 11924946,
            "range": "± 183301",
            "unit": "ns/iter"
          },
          {
            "name": "flood/bounded",
            "value": 13184919,
            "range": "± 145989",
            "unit": "ns/iter"
          },
          {
            "name": "ping_pong/pairs/1",
            "value": 652621,
            "range": "± 17109",
            "unit": "ns/iter"
          },
          {
            "name": "ping_pong/pairs/4",
            "value": 788719,
            "range": "± 16264",
            "unit": "ns/iter"
          },
          {
            "name": "fan_out/workers/4",
            "value": 4881300,
            "range": "± 114561",
            "unit": "ns/iter"
          },
          {
            "name": "fan_out/workers/16",
            "value": 3572866,
            "range": "± 20086",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "git@heikoseeberger.de",
            "name": "Heiko Seeberger",
            "username": "hseeberger"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "358c3fff403d4c4a381f585281ad9a55436cf26e",
          "message": "Merge pull request #126 from tellus-rs/chore/move-to-tellus-rs\n\nchore: move repo links and code owners to tellus-rs",
          "timestamp": "2026-10-17T21:02:28+02:00",
          "tree_id": "1b0039587d76b24bbe3e99f13e2fc5fdb7d1cf67",
          "url": "https://github.com/tellus-rs/tellus/commit/358c3fff403d4c4a381f585281ad9a55436cf26e"
        },
        "date": 1792263952942,
        "tool": "cargo",
        "benches": [
          {
            "name": "flood/unbounded",
            "value": 14224744,
            "range": "± 318362",
            "unit": "ns/iter"
          },
          {
            "name": "flood/bounded",
            "value": 12697633,
            "range": "± 72416",
            "unit": "ns/iter"
          },
          {
            "name": "ping_pong/pairs/1",
            "value": 639692,
            "range": "± 23682",
            "unit": "ns/iter"
          },
          {
            "name": "ping_pong/pairs/4",
            "value": 880322,
            "range": "± 7335",
            "unit": "ns/iter"
          },
          {
            "name": "fan_out/workers/4",
            "value": 5037039,
            "range": "± 130838",
            "unit": "ns/iter"
          },
          {
            "name": "fan_out/workers/16",
            "value": 3579139,
            "range": "± 21374",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "git@heikoseeberger.de",
            "name": "Heiko Seeberger",
            "username": "hseeberger"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "cda2e16ac484e64b0a2437aa333692c29ce7de99",
          "message": "Merge pull request #127 from tellus-rs/ci/trusted-publishing\n\nci: publish via crates.io trusted publishing, use app client id",
          "timestamp": "2026-10-17T21:17:50+02:00",
          "tree_id": "a2d750757f546f9bd71dd683305117df86f9e3c9",
          "url": "https://github.com/tellus-rs/tellus/commit/cda2e16ac484e64b0a2437aa333692c29ce7de99"
        },
        "date": 1792264868375,
        "tool": "cargo",
        "benches": [
          {
            "name": "flood/unbounded",
            "value": 7852655,
            "range": "± 260386",
            "unit": "ns/iter"
          },
          {
            "name": "flood/bounded",
            "value": 8586795,
            "range": "± 380773",
            "unit": "ns/iter"
          },
          {
            "name": "ping_pong/pairs/1",
            "value": 543740,
            "range": "± 13606",
            "unit": "ns/iter"
          },
          {
            "name": "ping_pong/pairs/4",
            "value": 521076,
            "range": "± 10278",
            "unit": "ns/iter"
          },
          {
            "name": "fan_out/workers/4",
            "value": 4138312,
            "range": "± 109496",
            "unit": "ns/iter"
          },
          {
            "name": "fan_out/workers/16",
            "value": 2599919,
            "range": "± 50466",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "git@heikoseeberger.de",
            "name": "Heiko Seeberger",
            "username": "hseeberger"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "1dd5dfb8bb7ed8d2338a1a6fb3fbde822a11d6dd",
          "message": "Merge pull request #125 from tellus-rs/release-plz-2026-10-17T18-42-27Z\n\nchore: release",
          "timestamp": "2026-10-17T21:25:01+02:00",
          "tree_id": "98f5ff9b799cdfe0efac6f2159d3338fea3e0aee",
          "url": "https://github.com/tellus-rs/tellus/commit/1dd5dfb8bb7ed8d2338a1a6fb3fbde822a11d6dd"
        },
        "date": 1792265309914,
        "tool": "cargo",
        "benches": [
          {
            "name": "flood/unbounded",
            "value": 10080605,
            "range": "± 353392",
            "unit": "ns/iter"
          },
          {
            "name": "flood/bounded",
            "value": 9187706,
            "range": "± 18874",
            "unit": "ns/iter"
          },
          {
            "name": "ping_pong/pairs/1",
            "value": 528506,
            "range": "± 5854",
            "unit": "ns/iter"
          },
          {
            "name": "ping_pong/pairs/4",
            "value": 652937,
            "range": "± 15113",
            "unit": "ns/iter"
          },
          {
            "name": "fan_out/workers/4",
            "value": 4117292,
            "range": "± 167460",
            "unit": "ns/iter"
          },
          {
            "name": "fan_out/workers/16",
            "value": 2854294,
            "range": "± 17648",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "git@heikoseeberger.de",
            "name": "Heiko Seeberger",
            "username": "hseeberger"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "e2ce1270bdc528c00f2ed28fb83482df6775b18f",
          "message": "Merge pull request #124 from tellus-rs/dependabot/github_actions/dtolnay/rust-toolchain-683b588c487a69d01f6c6ddf40f036ff682cc2c8\n\nci(deps): bump dtolnay/rust-toolchain from e2a55d2ffb04f378e9626c28d38b36d230d1e12f to 683b588c487a69d01f6c6ddf40f036ff682cc2c8",
          "timestamp": "2026-10-17T21:49:53+02:00",
          "tree_id": "f75801721ce40c89a34e59598ddd7da8ab44cef8",
          "url": "https://github.com/tellus-rs/tellus/commit/e2ce1270bdc528c00f2ed28fb83482df6775b18f"
        },
        "date": 1792266804132,
        "tool": "cargo",
        "benches": [
          {
            "name": "flood/unbounded",
            "value": 12549026,
            "range": "± 76202",
            "unit": "ns/iter"
          },
          {
            "name": "flood/bounded",
            "value": 12606344,
            "range": "± 141215",
            "unit": "ns/iter"
          },
          {
            "name": "ping_pong/pairs/1",
            "value": 651349,
            "range": "± 11006",
            "unit": "ns/iter"
          },
          {
            "name": "ping_pong/pairs/4",
            "value": 784942,
            "range": "± 5749",
            "unit": "ns/iter"
          },
          {
            "name": "fan_out/workers/4",
            "value": 4883900,
            "range": "± 77703",
            "unit": "ns/iter"
          },
          {
            "name": "fan_out/workers/16",
            "value": 3576714,
            "range": "± 19231",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "git@heikoseeberger.de",
            "name": "Heiko Seeberger",
            "username": "hseeberger"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "ec9697bd21db5b02a27db35bb277dc4587772e3b",
          "message": "Merge pull request #128 from tellus-rs/docs/tellus-rs-links\n\ndocs: point links and crate homepage to tellus.rs",
          "timestamp": "2026-10-18T00:08:16+02:00",
          "tree_id": "a17c13ca7f18a129df3fd03b817587a2dfbed380",
          "url": "https://github.com/tellus-rs/tellus/commit/ec9697bd21db5b02a27db35bb277dc4587772e3b"
        },
        "date": 1792275102188,
        "tool": "cargo",
        "benches": [
          {
            "name": "flood/unbounded",
            "value": 9455558,
            "range": "± 389985",
            "unit": "ns/iter"
          },
          {
            "name": "flood/bounded",
            "value": 11093466,
            "range": "± 73890",
            "unit": "ns/iter"
          },
          {
            "name": "ping_pong/pairs/1",
            "value": 521615,
            "range": "± 12378",
            "unit": "ns/iter"
          },
          {
            "name": "ping_pong/pairs/4",
            "value": 584661,
            "range": "± 2685",
            "unit": "ns/iter"
          },
          {
            "name": "fan_out/workers/4",
            "value": 4106481,
            "range": "± 123095",
            "unit": "ns/iter"
          },
          {
            "name": "fan_out/workers/16",
            "value": 2837252,
            "range": "± 16250",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "git@heikoseeberger.de",
            "name": "Heiko Seeberger",
            "username": "hseeberger"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "4893ce23ffdb54aa73c08e58944e5804af81685d",
          "message": "Merge pull request #130 from tellus-rs/docs/tutorial-getting-started\n\ndocs: unify the examples into a getting started tutorial",
          "timestamp": "2026-10-18T21:37:37+03:00",
          "tree_id": "b459381be11390a1580cdda86f9297f22869828f",
          "url": "https://github.com/tellus-rs/tellus/commit/4893ce23ffdb54aa73c08e58944e5804af81685d"
        },
        "date": 1792348859957,
        "tool": "cargo",
        "benches": [
          {
            "name": "flood/unbounded",
            "value": 12897983,
            "range": "± 167244",
            "unit": "ns/iter"
          },
          {
            "name": "flood/bounded",
            "value": 12941873,
            "range": "± 119050",
            "unit": "ns/iter"
          },
          {
            "name": "ping_pong/pairs/1",
            "value": 664288,
            "range": "± 25568",
            "unit": "ns/iter"
          },
          {
            "name": "ping_pong/pairs/4",
            "value": 842055,
            "range": "± 6686",
            "unit": "ns/iter"
          },
          {
            "name": "fan_out/workers/4",
            "value": 4936054,
            "range": "± 70736",
            "unit": "ns/iter"
          },
          {
            "name": "fan_out/workers/16",
            "value": 3542762,
            "range": "± 33687",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "git@heikoseeberger.de",
            "name": "Heiko Seeberger",
            "username": "hseeberger"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "ad1010dcdec074cba11439711cdbcdee953baac2",
          "message": "Merge pull request #131 from tellus-rs/release-plz-2026-10-18T18-39-53Z\n\nchore: release",
          "timestamp": "2026-10-18T21:48:34+03:00",
          "tree_id": "f3d71d7c3fab43a16795c30b7571f2d633d9d32f",
          "url": "https://github.com/tellus-rs/tellus/commit/ad1010dcdec074cba11439711cdbcdee953baac2"
        },
        "date": 1792349528961,
        "tool": "cargo",
        "benches": [
          {
            "name": "flood/unbounded",
            "value": 9614908,
            "range": "± 442297",
            "unit": "ns/iter"
          },
          {
            "name": "flood/bounded",
            "value": 10922658,
            "range": "± 255801",
            "unit": "ns/iter"
          },
          {
            "name": "ping_pong/pairs/1",
            "value": 518322,
            "range": "± 6049",
            "unit": "ns/iter"
          },
          {
            "name": "ping_pong/pairs/4",
            "value": 586367,
            "range": "± 3057",
            "unit": "ns/iter"
          },
          {
            "name": "fan_out/workers/4",
            "value": 4145142,
            "range": "± 127496",
            "unit": "ns/iter"
          },
          {
            "name": "fan_out/workers/16",
            "value": 2851679,
            "range": "± 16465",
            "unit": "ns/iter"
          }
        ]
      }
    ],
    "Persistence": [
      {
        "commit": {
          "author": {
            "email": "git@heikoseeberger.de",
            "name": "Heiko Seeberger",
            "username": "hseeberger"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "80e7177421e90a55b20e7cf1cbbc914903cc80ec",
          "message": "Merge pull request #21 from hseeberger/feat/persistence\n\nfeat: add event-sourced persistence",
          "timestamp": "2026-08-26T08:53:42+02:00",
          "tree_id": "98716b2cae996f3fa4c9ad53d94298f8920d71f0",
          "url": "https://github.com/hseeberger/tellus/commit/80e7177421e90a55b20e7cf1cbbc914903cc80ec"
        },
        "date": 1787727382366,
        "tool": "cargo",
        "benches": [
          {
            "name": "persist/no_snapshots",
            "value": 2963895,
            "range": "± 11288",
            "unit": "ns/iter"
          },
          {
            "name": "persist/snapshots",
            "value": 2916345,
            "range": "± 15207",
            "unit": "ns/iter"
          },
          {
            "name": "recover/replay",
            "value": 1755749,
            "range": "± 13886",
            "unit": "ns/iter"
          },
          {
            "name": "recover/snapshot",
            "value": 208540,
            "range": "± 6495",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "git@heikoseeberger.de",
            "name": "Heiko Seeberger",
            "username": "hseeberger"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "e51adc4128f5c749e297ca3b3cca92e28d707702",
          "message": "Merge pull request #47 from hseeberger/docs/refresh-after-persistence\n\ndocs: refresh docs after persistence landed",
          "timestamp": "2026-08-26T11:11:33+02:00",
          "tree_id": "25dc57378d1d9b392e4be70e040341db6a59bfdb",
          "url": "https://github.com/hseeberger/tellus/commit/e51adc4128f5c749e297ca3b3cca92e28d707702"
        },
        "date": 1787735652483,
        "tool": "cargo",
        "benches": [
          {
            "name": "persist/no_snapshots",
            "value": 3010148,
            "range": "± 10571",
            "unit": "ns/iter"
          },
          {
            "name": "persist/snapshots",
            "value": 2941202,
            "range": "± 5354",
            "unit": "ns/iter"
          },
          {
            "name": "recover/replay",
            "value": 1632694,
            "range": "± 7700",
            "unit": "ns/iter"
          },
          {
            "name": "recover/snapshot",
            "value": 210074,
            "range": "± 3640",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "git@heikoseeberger.de",
            "name": "Heiko Seeberger",
            "username": "hseeberger"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "122046c900e5309a4567d0a13b3532cb9a4ab17a",
          "message": "Merge pull request #49 from hseeberger/build/publish-persistence-postgres\n\nbuild: publish tellus-persistence-postgres to crates.io",
          "timestamp": "2026-08-26T11:29:52+02:00",
          "tree_id": "29a63385a27e372b211c583741b956b1e1ccbec7",
          "url": "https://github.com/hseeberger/tellus/commit/122046c900e5309a4567d0a13b3532cb9a4ab17a"
        },
        "date": 1787736713568,
        "tool": "cargo",
        "benches": [
          {
            "name": "persist/no_snapshots",
            "value": 3202949,
            "range": "± 12756",
            "unit": "ns/iter"
          },
          {
            "name": "persist/snapshots",
            "value": 3271057,
            "range": "± 30443",
            "unit": "ns/iter"
          },
          {
            "name": "recover/replay",
            "value": 1535906,
            "range": "± 2947",
            "unit": "ns/iter"
          },
          {
            "name": "recover/snapshot",
            "value": 184317,
            "range": "± 1025",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "git@heikoseeberger.de",
            "name": "Heiko Seeberger",
            "username": "hseeberger"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "0f679455839c1ea6d716352859a2dcd95ed40711",
          "message": "Merge pull request #51 from hseeberger/docs/cargo-add-install\n\ndocs: install via cargo add instead of pinned versions",
          "timestamp": "2026-08-27T17:42:04+02:00",
          "tree_id": "95736cb2e525bf5f787649f3157aa160bfe0b767",
          "url": "https://github.com/hseeberger/tellus/commit/0f679455839c1ea6d716352859a2dcd95ed40711"
        },
        "date": 1787845460631,
        "tool": "cargo",
        "benches": [
          {
            "name": "persist/no_snapshots",
            "value": 3404791,
            "range": "± 52937",
            "unit": "ns/iter"
          },
          {
            "name": "persist/snapshots",
            "value": 3385909,
            "range": "± 47930",
            "unit": "ns/iter"
          },
          {
            "name": "recover/replay",
            "value": 1311595,
            "range": "± 4937",
            "unit": "ns/iter"
          },
          {
            "name": "recover/snapshot",
            "value": 167741,
            "range": "± 3752",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "git@heikoseeberger.de",
            "name": "Heiko Seeberger",
            "username": "hseeberger"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "779ef2f79d25a8532753cbbb110a8b93ce8bcbb9",
          "message": "Merge pull request #52 from hseeberger/build/independent-crate-versions\n\nbuild: use independent versions per crate",
          "timestamp": "2026-08-27T17:53:17+02:00",
          "tree_id": "bb4b2115dc6995dd7741bc79ba515d70bc94d278",
          "url": "https://github.com/hseeberger/tellus/commit/779ef2f79d25a8532753cbbb110a8b93ce8bcbb9"
        },
        "date": 1787846116378,
        "tool": "cargo",
        "benches": [
          {
            "name": "persist/no_snapshots",
            "value": 2517560,
            "range": "± 26564",
            "unit": "ns/iter"
          },
          {
            "name": "persist/snapshots",
            "value": 2639646,
            "range": "± 20860",
            "unit": "ns/iter"
          },
          {
            "name": "recover/replay",
            "value": 1195492,
            "range": "± 6157",
            "unit": "ns/iter"
          },
          {
            "name": "recover/snapshot",
            "value": 141635,
            "range": "± 3048",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "git@heikoseeberger.de",
            "name": "Heiko Seeberger",
            "username": "hseeberger"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "105234ca3f6a7f3c36f7bb95f8bb8b1fef19e829",
          "message": "Merge pull request #54 from hseeberger/release/tellus-0.2.0\n\nchore: release tellus 0.2.0 and tellus-persistence-postgres 0.1.0",
          "timestamp": "2026-08-27T23:33:37+02:00",
          "tree_id": "173bf718a78bdb32ab258f04cd95f1f2a59eefda",
          "url": "https://github.com/hseeberger/tellus/commit/105234ca3f6a7f3c36f7bb95f8bb8b1fef19e829"
        },
        "date": 1787866535284,
        "tool": "cargo",
        "benches": [
          {
            "name": "persist/no_snapshots",
            "value": 2457849,
            "range": "± 20934",
            "unit": "ns/iter"
          },
          {
            "name": "persist/snapshots",
            "value": 2413849,
            "range": "± 6395",
            "unit": "ns/iter"
          },
          {
            "name": "recover/replay",
            "value": 1226885,
            "range": "± 9473",
            "unit": "ns/iter"
          },
          {
            "name": "recover/snapshot",
            "value": 149603,
            "range": "± 5594",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "git@heikoseeberger.de",
            "name": "Heiko Seeberger",
            "username": "hseeberger"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "f7e8a3ec45a0f24d8c8bbc25a3a8f12714245fcf",
          "message": "Merge pull request #55 from hseeberger/chore/bench-variance\n\nchore: pin bench runtime threads and raise sampling",
          "timestamp": "2026-08-28T00:03:23+02:00",
          "tree_id": "e70ac7f0b854adff8d7a19ee4fe80d46869ed424",
          "url": "https://github.com/hseeberger/tellus/commit/f7e8a3ec45a0f24d8c8bbc25a3a8f12714245fcf"
        },
        "date": 1787868417430,
        "tool": "cargo",
        "benches": [
          {
            "name": "persist/no_snapshots",
            "value": 3039433,
            "range": "± 19013",
            "unit": "ns/iter"
          },
          {
            "name": "persist/snapshots",
            "value": 2934554,
            "range": "± 12096",
            "unit": "ns/iter"
          },
          {
            "name": "recover/replay",
            "value": 1629315,
            "range": "± 8422",
            "unit": "ns/iter"
          },
          {
            "name": "recover/snapshot",
            "value": 209484,
            "range": "± 6380",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "git@heikoseeberger.de",
            "name": "Heiko Seeberger",
            "username": "hseeberger"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "9392dec4a4ce622e105058bcd1c0f7f1804c001e",
          "message": "Merge pull request #56 from hseeberger/ci/no-tag-trigger\n\nci: drop tag trigger from CI workflow",
          "timestamp": "2026-08-28T00:13:21+02:00",
          "tree_id": "e99834098d29fbda0b78763d4c6c5488c76f4cc0",
          "url": "https://github.com/hseeberger/tellus/commit/9392dec4a4ce622e105058bcd1c0f7f1804c001e"
        },
        "date": 1787869013244,
        "tool": "cargo",
        "benches": [
          {
            "name": "persist/no_snapshots",
            "value": 3142533,
            "range": "± 20834",
            "unit": "ns/iter"
          },
          {
            "name": "persist/snapshots",
            "value": 3112321,
            "range": "± 28054",
            "unit": "ns/iter"
          },
          {
            "name": "recover/replay",
            "value": 1484287,
            "range": "± 3327",
            "unit": "ns/iter"
          },
          {
            "name": "recover/snapshot",
            "value": 190920,
            "range": "± 6711",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "git@heikoseeberger.de",
            "name": "Heiko Seeberger",
            "username": "hseeberger"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "459ca7503272e953ef66a61aa0f47d6732a6b42c",
          "message": "Merge pull request #59 from hseeberger/ci/informational-bench-gate\n\nci: make bench comparison informational",
          "timestamp": "2026-08-28T11:03:13+02:00",
          "tree_id": "dcc0235bc8c9ae9325d2f84a4d24a06a0b774da1",
          "url": "https://github.com/hseeberger/tellus/commit/459ca7503272e953ef66a61aa0f47d6732a6b42c"
        },
        "date": 1787908009847,
        "tool": "cargo",
        "benches": [
          {
            "name": "persist/no_snapshots",
            "value": 3017230,
            "range": "± 26024",
            "unit": "ns/iter"
          },
          {
            "name": "persist/snapshots",
            "value": 3133426,
            "range": "± 41251",
            "unit": "ns/iter"
          },
          {
            "name": "recover/replay",
            "value": 1641699,
            "range": "± 11941",
            "unit": "ns/iter"
          },
          {
            "name": "recover/snapshot",
            "value": 205240,
            "range": "± 6712",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "git@heikoseeberger.de",
            "name": "Heiko Seeberger",
            "username": "hseeberger"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "000d5bb542ddea72395185a6e662ebe2e136c94d",
          "message": "Merge pull request #61 from hseeberger/refactor/reduce-nesting-and-duplication\n\nrefactor: cut nesting and duplication in core and persistence",
          "timestamp": "2026-08-28T11:53:48+02:00",
          "tree_id": "b661c51c56c02fe96cb8f02b1e17b1389c161b2a",
          "url": "https://github.com/hseeberger/tellus/commit/000d5bb542ddea72395185a6e662ebe2e136c94d"
        },
        "date": 1787911032372,
        "tool": "cargo",
        "benches": [
          {
            "name": "persist/no_snapshots",
            "value": 3137067,
            "range": "± 17878",
            "unit": "ns/iter"
          },
          {
            "name": "persist/snapshots",
            "value": 3055988,
            "range": "± 14048",
            "unit": "ns/iter"
          },
          {
            "name": "recover/replay",
            "value": 1768014,
            "range": "± 7684",
            "unit": "ns/iter"
          },
          {
            "name": "recover/snapshot",
            "value": 204812,
            "range": "± 6855",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "git@heikoseeberger.de",
            "name": "Heiko Seeberger",
            "username": "hseeberger"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "92fa3b15bc5d904aab4a8d95a1b7171117f55582",
          "message": "Merge pull request #62 from hseeberger/refactor/harden-core-and-persistence\n\nrefactor: harden core and persistence guarantees",
          "timestamp": "2026-08-29T23:01:57+02:00",
          "tree_id": "91b0982e85d89aefa13f11991c36e87b39fe737d",
          "url": "https://github.com/hseeberger/tellus/commit/92fa3b15bc5d904aab4a8d95a1b7171117f55582"
        },
        "date": 1788037533136,
        "tool": "cargo",
        "benches": [
          {
            "name": "persist/no_snapshots",
            "value": 3011305,
            "range": "± 28573",
            "unit": "ns/iter"
          },
          {
            "name": "persist/snapshots",
            "value": 3004586,
            "range": "± 24316",
            "unit": "ns/iter"
          },
          {
            "name": "recover/replay",
            "value": 1763765,
            "range": "± 6745",
            "unit": "ns/iter"
          },
          {
            "name": "recover/snapshot",
            "value": 214631,
            "range": "± 6350",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "git@heikoseeberger.de",
            "name": "Heiko Seeberger",
            "username": "hseeberger"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "8b55230f8f5224e73fdd7e316b20a4ef3807f4ab",
          "message": "Merge pull request #63 from hseeberger/fix/backoff-recovery-bench\n\nfix: harden backoff config, recovery drop and bench sizing",
          "timestamp": "2026-08-30T10:27:24+02:00",
          "tree_id": "752ffdbbab3300c992bee0982ffe325d4e4010c1",
          "url": "https://github.com/hseeberger/tellus/commit/8b55230f8f5224e73fdd7e316b20a4ef3807f4ab"
        },
        "date": 1788078649027,
        "tool": "cargo",
        "benches": [
          {
            "name": "persist/no_snapshots",
            "value": 2430662,
            "range": "± 5874",
            "unit": "ns/iter"
          },
          {
            "name": "persist/snapshots",
            "value": 2422025,
            "range": "± 150713",
            "unit": "ns/iter"
          },
          {
            "name": "recover/replay",
            "value": 1195966,
            "range": "± 3730",
            "unit": "ns/iter"
          },
          {
            "name": "recover/snapshot",
            "value": 142685,
            "range": "± 4070",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "git@heikoseeberger.de",
            "name": "Heiko Seeberger",
            "username": "hseeberger"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "c8ffcccd00d6cb31fb101ac0b2dac89ac987ca5a",
          "message": "Merge pull request #60 from hseeberger/release-plz-2026-08-28T09-04-03Z\n\nchore: release",
          "timestamp": "2026-08-31T12:27:04+02:00",
          "tree_id": "cfef77b6fcf556c46845948c805b148e6f3b12a3",
          "url": "https://github.com/hseeberger/tellus/commit/c8ffcccd00d6cb31fb101ac0b2dac89ac987ca5a"
        },
        "date": 1788172237155,
        "tool": "cargo",
        "benches": [
          {
            "name": "persist/no_snapshots",
            "value": 3069794,
            "range": "± 29502",
            "unit": "ns/iter"
          },
          {
            "name": "persist/snapshots",
            "value": 3024961,
            "range": "± 51294",
            "unit": "ns/iter"
          },
          {
            "name": "recover/replay",
            "value": 1757559,
            "range": "± 9376",
            "unit": "ns/iter"
          },
          {
            "name": "recover/snapshot",
            "value": 210660,
            "range": "± 3729",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "git@heikoseeberger.de",
            "name": "Heiko Seeberger",
            "username": "hseeberger"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "0745187b2fffc1e5e98f51a5fbc429d538044cfe",
          "message": "Merge pull request #64 from hseeberger/dependabot/cargo/cargo-major-a925caaff2\n\nbuild(deps): bump hotpath from 0.23.3 to 0.24.0 in the cargo-major group across 1 directory",
          "timestamp": "2026-09-01T21:39:40+02:00",
          "tree_id": "6a6692047a3bd9542ec6452d5e1a2cf79b488561",
          "url": "https://github.com/hseeberger/tellus/commit/0745187b2fffc1e5e98f51a5fbc429d538044cfe"
        },
        "date": 1788291788048,
        "tool": "cargo",
        "benches": [
          {
            "name": "persist/no_snapshots",
            "value": 2493191,
            "range": "± 24377",
            "unit": "ns/iter"
          },
          {
            "name": "persist/snapshots",
            "value": 2473554,
            "range": "± 8277",
            "unit": "ns/iter"
          },
          {
            "name": "recover/replay",
            "value": 1059565,
            "range": "± 4625",
            "unit": "ns/iter"
          },
          {
            "name": "recover/snapshot",
            "value": 141570,
            "range": "± 3335",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "git@heikoseeberger.de",
            "name": "Heiko Seeberger",
            "username": "hseeberger"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "c008766f415ed9c4848b94b133c591e936ce451d",
          "message": "Merge pull request #67 from hseeberger/build/cargo-hack\n\nbuild: drive the check and lint feature matrix with cargo-hack",
          "timestamp": "2026-09-04T14:22:16+02:00",
          "tree_id": "f7c785cc5e1f96489bbbdd78fbc2b8cce6c595cc",
          "url": "https://github.com/hseeberger/tellus/commit/c008766f415ed9c4848b94b133c591e936ce451d"
        },
        "date": 1788524744267,
        "tool": "cargo",
        "benches": [
          {
            "name": "persist/no_snapshots",
            "value": 2830831,
            "range": "± 132233",
            "unit": "ns/iter"
          },
          {
            "name": "persist/snapshots",
            "value": 2538153,
            "range": "± 65590",
            "unit": "ns/iter"
          },
          {
            "name": "recover/replay",
            "value": 957126,
            "range": "± 29583",
            "unit": "ns/iter"
          },
          {
            "name": "recover/snapshot",
            "value": 132039,
            "range": "± 5776",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "git@heikoseeberger.de",
            "name": "Heiko Seeberger",
            "username": "hseeberger"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "f3996e8d5447cd1961ddcafa7881a7a1ea606af7",
          "message": "Merge pull request #68 from hseeberger/dependabot/cargo/cargo-minor-8d2c85db1c\n\nbuild(deps): bump uuid from 1.25.0 to 1.26.0 in the cargo-minor group",
          "timestamp": "2026-09-08T21:26:44+02:00",
          "tree_id": "11572b6facd660d429236b72c801d2fd9c4f8f16",
          "url": "https://github.com/hseeberger/tellus/commit/f3996e8d5447cd1961ddcafa7881a7a1ea606af7"
        },
        "date": 1788895845348,
        "tool": "cargo",
        "benches": [
          {
            "name": "persist/no_snapshots",
            "value": 3106725,
            "range": "± 18904",
            "unit": "ns/iter"
          },
          {
            "name": "persist/snapshots",
            "value": 3036835,
            "range": "± 24106",
            "unit": "ns/iter"
          },
          {
            "name": "recover/replay",
            "value": 1620552,
            "range": "± 3687",
            "unit": "ns/iter"
          },
          {
            "name": "recover/snapshot",
            "value": 205842,
            "range": "± 3787",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "git@heikoseeberger.de",
            "name": "Heiko Seeberger",
            "username": "hseeberger"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "777d77585d3af2440b768f780df2466e2019e8e6",
          "message": "Merge pull request #69 from hseeberger/dependabot/github_actions/ci-minor-fcbeee0005\n\nci(deps): bump taiki-e/install-action from 2.86.7 to 2.87.3 in the ci-minor group",
          "timestamp": "2026-09-08T21:32:03+02:00",
          "tree_id": "da673ef3816b3932f0b189fc920af9ce8c0a2f8b",
          "url": "https://github.com/hseeberger/tellus/commit/777d77585d3af2440b768f780df2466e2019e8e6"
        },
        "date": 1788896128710,
        "tool": "cargo",
        "benches": [
          {
            "name": "persist/no_snapshots",
            "value": 3042487,
            "range": "± 21852",
            "unit": "ns/iter"
          },
          {
            "name": "persist/snapshots",
            "value": 3043121,
            "range": "± 12440",
            "unit": "ns/iter"
          },
          {
            "name": "recover/replay",
            "value": 1765912,
            "range": "± 8178",
            "unit": "ns/iter"
          },
          {
            "name": "recover/snapshot",
            "value": 205532,
            "range": "± 3538",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "git@heikoseeberger.de",
            "name": "Heiko Seeberger",
            "username": "hseeberger"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "521e3495b152643addd15034394faf9ba8215200",
          "message": "Merge pull request #70 from hseeberger/build/rust\n\nbuild(deps): bump Rust to 1.98.1",
          "timestamp": "2026-09-10T15:07:59+03:00",
          "tree_id": "754e22e20c6c5fc17d3cfd2920c596b3f8665b40",
          "url": "https://github.com/hseeberger/tellus/commit/521e3495b152643addd15034394faf9ba8215200"
        },
        "date": 1789042314527,
        "tool": "cargo",
        "benches": [
          {
            "name": "persist/no_snapshots",
            "value": 2464629,
            "range": "± 21785",
            "unit": "ns/iter"
          },
          {
            "name": "persist/snapshots",
            "value": 2418250,
            "range": "± 13262",
            "unit": "ns/iter"
          },
          {
            "name": "recover/replay",
            "value": 1194610,
            "range": "± 5418",
            "unit": "ns/iter"
          },
          {
            "name": "recover/snapshot",
            "value": 141287,
            "range": "± 3988",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "git@heikoseeberger.de",
            "name": "Heiko Seeberger",
            "username": "hseeberger"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "f0a42030e28cbc1a66090cefda4d1c2b836939c2",
          "message": "Merge pull request #72 from hseeberger/feat/actor-system-stop\n\nfeat: stop an actor system from the outside",
          "timestamp": "2026-09-13T11:03:13+03:00",
          "tree_id": "0489674a649d1aac43dfc1308fc89f3b6e35bf9e",
          "url": "https://github.com/hseeberger/tellus/commit/f0a42030e28cbc1a66090cefda4d1c2b836939c2"
        },
        "date": 1789286846985,
        "tool": "cargo",
        "benches": [
          {
            "name": "persist/no_snapshots",
            "value": 3088153,
            "range": "± 17263",
            "unit": "ns/iter"
          },
          {
            "name": "persist/snapshots",
            "value": 3039018,
            "range": "± 39100",
            "unit": "ns/iter"
          },
          {
            "name": "recover/replay",
            "value": 1758410,
            "range": "± 8560",
            "unit": "ns/iter"
          },
          {
            "name": "recover/snapshot",
            "value": 213984,
            "range": "± 6314",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "git@heikoseeberger.de",
            "name": "Heiko Seeberger",
            "username": "hseeberger"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "48ce2a29421baad394de223b318f6d27e3b9ff14",
          "message": "Merge pull request #74 from hseeberger/feat/in-memory-store\n\nfeat: add in-memory store behind persistence-in-memory feature",
          "timestamp": "2026-09-13T17:58:37+03:00",
          "tree_id": "c35b0d128021ae17191aeb230676e9864508537f",
          "url": "https://github.com/hseeberger/tellus/commit/48ce2a29421baad394de223b318f6d27e3b9ff14"
        },
        "date": 1789311723126,
        "tool": "cargo",
        "benches": [
          {
            "name": "persist/no_snapshots",
            "value": 3019769,
            "range": "± 23240",
            "unit": "ns/iter"
          },
          {
            "name": "persist/snapshots",
            "value": 3030716,
            "range": "± 36471",
            "unit": "ns/iter"
          },
          {
            "name": "recover/replay",
            "value": 1758035,
            "range": "± 5329",
            "unit": "ns/iter"
          },
          {
            "name": "recover/snapshot",
            "value": 209498,
            "range": "± 5478",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "git@heikoseeberger.de",
            "name": "Heiko Seeberger",
            "username": "hseeberger"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "48e01ffd17482451cf6b59345b380eb0c6084932",
          "message": "Merge pull request #73 from hseeberger/feat/host\n\nfeat: add hosts, spawning/passivating managed entities",
          "timestamp": "2026-09-13T18:54:26+03:00",
          "tree_id": "f16b3b187a342ef5f7a4c11d6e20d9f65225237b",
          "url": "https://github.com/hseeberger/tellus/commit/48e01ffd17482451cf6b59345b380eb0c6084932"
        },
        "date": 1789315075592,
        "tool": "cargo",
        "benches": [
          {
            "name": "persist/no_snapshots",
            "value": 3103087,
            "range": "± 13839",
            "unit": "ns/iter"
          },
          {
            "name": "persist/snapshots",
            "value": 3010981,
            "range": "± 17024",
            "unit": "ns/iter"
          },
          {
            "name": "recover/replay",
            "value": 1642241,
            "range": "± 11641",
            "unit": "ns/iter"
          },
          {
            "name": "recover/snapshot",
            "value": 211293,
            "range": "± 14249",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "git@heikoseeberger.de",
            "name": "Heiko Seeberger",
            "username": "hseeberger"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "4b988a401be5cdffb487c0cf1abffaf8d15d7970",
          "message": "Merge pull request #76 from hseeberger/release-plz-2026-09-13T15-55-22Z\n\nchore: release",
          "timestamp": "2026-09-13T19:22:05+03:00",
          "tree_id": "103de6f97c2fe22571dfd29cf6d126d276770610",
          "url": "https://github.com/hseeberger/tellus/commit/4b988a401be5cdffb487c0cf1abffaf8d15d7970"
        },
        "date": 1789316731512,
        "tool": "cargo",
        "benches": [
          {
            "name": "persist/no_snapshots",
            "value": 3099601,
            "range": "± 57477",
            "unit": "ns/iter"
          },
          {
            "name": "persist/snapshots",
            "value": 3080337,
            "range": "± 34581",
            "unit": "ns/iter"
          },
          {
            "name": "recover/replay",
            "value": 1641151,
            "range": "± 5625",
            "unit": "ns/iter"
          },
          {
            "name": "recover/snapshot",
            "value": 208366,
            "range": "± 6567",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "git@heikoseeberger.de",
            "name": "Heiko Seeberger",
            "username": "hseeberger"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "9a7e5020681c36622c2277dca02f50970f4ad0a4",
          "message": "Merge pull request #77 from hseeberger/test/faulty-store\n\ntest: replace TestStore with FaultyStore over InMemoryStore",
          "timestamp": "2026-09-13T22:01:09+03:00",
          "tree_id": "5625602f3c93c03e3e2c5c5266686129f81c94a9",
          "url": "https://github.com/hseeberger/tellus/commit/9a7e5020681c36622c2277dca02f50970f4ad0a4"
        },
        "date": 1789326285830,
        "tool": "cargo",
        "benches": [
          {
            "name": "persist/no_snapshots",
            "value": 3072361,
            "range": "± 126171",
            "unit": "ns/iter"
          },
          {
            "name": "persist/snapshots",
            "value": 3048555,
            "range": "± 35081",
            "unit": "ns/iter"
          },
          {
            "name": "recover/replay",
            "value": 1106865,
            "range": "± 13377",
            "unit": "ns/iter"
          },
          {
            "name": "recover/snapshot",
            "value": 139855,
            "range": "± 5204",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "git@heikoseeberger.de",
            "name": "Heiko Seeberger",
            "username": "hseeberger"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "d1e9362f7359ad8d474fd888e6b5df3ebb3c12db",
          "message": "Merge pull request #80 from hseeberger/dependabot/cargo/cargo-major-dc3657b4df\n\nbuild(deps): bump hotpath from 0.24.0 to 0.25.1 in the cargo-major group",
          "timestamp": "2026-09-15T21:37:26+03:00",
          "tree_id": "449df767d4c6166fe24893b783ad3fd3ab0b5d32",
          "url": "https://github.com/hseeberger/tellus/commit/d1e9362f7359ad8d474fd888e6b5df3ebb3c12db"
        },
        "date": 1789497658262,
        "tool": "cargo",
        "benches": [
          {
            "name": "persist/no_snapshots",
            "value": 2811410,
            "range": "± 88776",
            "unit": "ns/iter"
          },
          {
            "name": "persist/snapshots",
            "value": 2530334,
            "range": "± 124243",
            "unit": "ns/iter"
          },
          {
            "name": "recover/replay",
            "value": 909680,
            "range": "± 50927",
            "unit": "ns/iter"
          },
          {
            "name": "recover/snapshot",
            "value": 125035,
            "range": "± 8684",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "git@heikoseeberger.de",
            "name": "Heiko Seeberger",
            "username": "hseeberger"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "907e5dd18f73d8981cbfa249563e842a80d321ec",
          "message": "Merge pull request #84 from hseeberger/fix/hotpath-utils-version\n\nci: install hotpath-utils matching the hotpath version",
          "timestamp": "2026-09-16T09:39:34+03:00",
          "tree_id": "2542b23d7ffac9d852bf76ed17599bb220c982dd",
          "url": "https://github.com/hseeberger/tellus/commit/907e5dd18f73d8981cbfa249563e842a80d321ec"
        },
        "date": 1789540977990,
        "tool": "cargo",
        "benches": [
          {
            "name": "persist/no_snapshots",
            "value": 3081784,
            "range": "± 4000",
            "unit": "ns/iter"
          },
          {
            "name": "persist/snapshots",
            "value": 3105044,
            "range": "± 19480",
            "unit": "ns/iter"
          },
          {
            "name": "recover/replay",
            "value": 1752036,
            "range": "± 8278",
            "unit": "ns/iter"
          },
          {
            "name": "recover/snapshot",
            "value": 211311,
            "range": "± 4965",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "git@heikoseeberger.de",
            "name": "Heiko Seeberger",
            "username": "hseeberger"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "bf073f6f16b533145ebc9e0a22f7256356857268",
          "message": "Merge pull request #83 from hseeberger/fix/79-rustsec-2026-0285\n\nfix(deps): bump rustls to 0.23.45 for RUSTSEC-2026-0285",
          "timestamp": "2026-09-16T09:46:58+03:00",
          "tree_id": "1da2d3fe3bb0592c7c23fd2a904116819332babc",
          "url": "https://github.com/hseeberger/tellus/commit/bf073f6f16b533145ebc9e0a22f7256356857268"
        },
        "date": 1789541431475,
        "tool": "cargo",
        "benches": [
          {
            "name": "persist/no_snapshots",
            "value": 3068583,
            "range": "± 31333",
            "unit": "ns/iter"
          },
          {
            "name": "persist/snapshots",
            "value": 3062212,
            "range": "± 21867",
            "unit": "ns/iter"
          },
          {
            "name": "recover/replay",
            "value": 1631616,
            "range": "± 18807",
            "unit": "ns/iter"
          },
          {
            "name": "recover/snapshot",
            "value": 205746,
            "range": "± 3672",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "git@heikoseeberger.de",
            "name": "Heiko Seeberger",
            "username": "hseeberger"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "108c7b45ee230b0d3d8a6cd667a887c743f5e95c",
          "message": "Merge pull request #89 from hseeberger/dependabot/github_actions/dtolnay/rust-toolchain-02cb101ec7c40f2c49e1d9714d64511d8e1b74de\n\nci(deps): bump dtolnay/rust-toolchain from 6c977a6ca4077a0ceb28ffbe03f59d46e9ac8772 to 02cb101ec7c40f2c49e1d9714d64511d8e1b74de",
          "timestamp": "2026-09-23T00:22:23+03:00",
          "tree_id": "6acc43375531f2cabd3250fa2af30f4f80fa6790",
          "url": "https://github.com/hseeberger/tellus/commit/108c7b45ee230b0d3d8a6cd667a887c743f5e95c"
        },
        "date": 1790112354309,
        "tool": "cargo",
        "benches": [
          {
            "name": "persist/no_snapshots",
            "value": 3152585,
            "range": "± 7275",
            "unit": "ns/iter"
          },
          {
            "name": "persist/snapshots",
            "value": 3030491,
            "range": "± 37997",
            "unit": "ns/iter"
          },
          {
            "name": "recover/replay",
            "value": 1776760,
            "range": "± 9613",
            "unit": "ns/iter"
          },
          {
            "name": "recover/snapshot",
            "value": 217654,
            "range": "± 4559",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "git@heikoseeberger.de",
            "name": "Heiko Seeberger",
            "username": "hseeberger"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "fe53a3210d3e3f716b430a974880a2bc0ffb3659",
          "message": "Merge pull request #91 from hseeberger/dependabot/cargo/cargo-major-f1df897d83\n\nbuild(deps): bump hotpath from 0.25.1 to 0.26.0 in the cargo-major group",
          "timestamp": "2026-09-30T11:10:34+02:00",
          "tree_id": "24296d02809b554a7ae016d2020c7ffe8d25763f",
          "url": "https://github.com/hseeberger/tellus/commit/fe53a3210d3e3f716b430a974880a2bc0ffb3659"
        },
        "date": 1790759658162,
        "tool": "cargo",
        "benches": [
          {
            "name": "persist/no_snapshots",
            "value": 3041897,
            "range": "± 11247",
            "unit": "ns/iter"
          },
          {
            "name": "persist/snapshots",
            "value": 3024305,
            "range": "± 18830",
            "unit": "ns/iter"
          },
          {
            "name": "recover/replay",
            "value": 1645464,
            "range": "± 29297",
            "unit": "ns/iter"
          },
          {
            "name": "recover/snapshot",
            "value": 206727,
            "range": "± 5529",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "git@heikoseeberger.de",
            "name": "Heiko Seeberger",
            "username": "hseeberger"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "572c0f2100953f753c69a94824f7c988cb3f232a",
          "message": "Merge pull request #98 from hseeberger/dependabot/github_actions/dtolnay/rust-toolchain-7e38f4b43b4db5c8dd498af069a4f6196df1d067\n\nci(deps): bump dtolnay/rust-toolchain from 02cb101ec7c40f2c49e1d9714d64511d8e1b74de to 7e38f4b43b4db5c8dd498af069a4f6196df1d067",
          "timestamp": "2026-10-08T11:43:19+02:00",
          "tree_id": "61c0b543eee8c8666a198596fd3919c159d68509",
          "url": "https://github.com/hseeberger/tellus/commit/572c0f2100953f753c69a94824f7c988cb3f232a"
        },
        "date": 1791452836301,
        "tool": "cargo",
        "benches": [
          {
            "name": "persist/no_snapshots",
            "value": 2489316,
            "range": "± 22837",
            "unit": "ns/iter"
          },
          {
            "name": "persist/snapshots",
            "value": 2496525,
            "range": "± 23307",
            "unit": "ns/iter"
          },
          {
            "name": "recover/replay",
            "value": 1062307,
            "range": "± 6393",
            "unit": "ns/iter"
          },
          {
            "name": "recover/snapshot",
            "value": 144400,
            "range": "± 4124",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "git@heikoseeberger.de",
            "name": "Heiko Seeberger",
            "username": "hseeberger"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "1b0d058ddd53a1d1b41287f8c867fe32e74d1352",
          "message": "Merge pull request #96 from hseeberger/dependabot/cargo/cargo-major-4e228f18ff\n\nbuild(deps): bump hotpath from 0.26.1 to 0.28.0 in the cargo-major group across 1 directory",
          "timestamp": "2026-10-08T11:52:16+02:00",
          "tree_id": "ea605ca56da5c5c004141fb6171c5dd9e303b20e",
          "url": "https://github.com/hseeberger/tellus/commit/1b0d058ddd53a1d1b41287f8c867fe32e74d1352"
        },
        "date": 1791453361077,
        "tool": "cargo",
        "benches": [
          {
            "name": "persist/no_snapshots",
            "value": 2166420,
            "range": "± 51203",
            "unit": "ns/iter"
          },
          {
            "name": "persist/snapshots",
            "value": 2211202,
            "range": "± 41944",
            "unit": "ns/iter"
          },
          {
            "name": "recover/replay",
            "value": 1210439,
            "range": "± 13750",
            "unit": "ns/iter"
          },
          {
            "name": "recover/snapshot",
            "value": 138965,
            "range": "± 4303",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "git@heikoseeberger.de",
            "name": "Heiko Seeberger",
            "username": "hseeberger"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "fada3f8a503ccf0486b49e9165ff8d1982694a3a",
          "message": "Merge pull request #99 from pawurb/hotpath-cloud\n\nci: migrate profiling to hotpath cloud",
          "timestamp": "2026-10-10T16:59:06+02:00",
          "tree_id": "c7de65dfa3ef506f0adb63107b1596577e43561e",
          "url": "https://github.com/hseeberger/tellus/commit/fada3f8a503ccf0486b49e9165ff8d1982694a3a"
        },
        "date": 1791644551059,
        "tool": "cargo",
        "benches": [
          {
            "name": "persist/no_snapshots",
            "value": 3149559,
            "range": "± 140429",
            "unit": "ns/iter"
          },
          {
            "name": "persist/snapshots",
            "value": 3090514,
            "range": "± 20060",
            "unit": "ns/iter"
          },
          {
            "name": "recover/replay",
            "value": 1766292,
            "range": "± 9245",
            "unit": "ns/iter"
          },
          {
            "name": "recover/snapshot",
            "value": 214208,
            "range": "± 5441",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "git@heikoseeberger.de",
            "name": "Heiko Seeberger",
            "username": "hseeberger"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "4c1ea36554936b83207a6697b050f692aa4c21ee",
          "message": "Merge pull request #104 from hseeberger/ci/drop-alloc-gate-retry\n\nci: drop the zero-alloc gate retry now that hotpath is fixed",
          "timestamp": "2026-10-10T19:03:17+02:00",
          "tree_id": "0dfd00729ceb4b54c4a4622819a80a0c6e7bc8f1",
          "url": "https://github.com/hseeberger/tellus/commit/4c1ea36554936b83207a6697b050f692aa4c21ee"
        },
        "date": 1791652003596,
        "tool": "cargo",
        "benches": [
          {
            "name": "persist/no_snapshots",
            "value": 2171910,
            "range": "± 41525",
            "unit": "ns/iter"
          },
          {
            "name": "persist/snapshots",
            "value": 2120621,
            "range": "± 52104",
            "unit": "ns/iter"
          },
          {
            "name": "recover/replay",
            "value": 1201868,
            "range": "± 23755",
            "unit": "ns/iter"
          },
          {
            "name": "recover/snapshot",
            "value": 140294,
            "range": "± 5983",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "git@heikoseeberger.de",
            "name": "Heiko Seeberger",
            "username": "hseeberger"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "1cfc9e5d3904714cc4b7667ad8f631f459931d47",
          "message": "Merge pull request #103 from hseeberger/feat/test-context\n\nfeat: add TestContext behind test-util for unit testing actors",
          "timestamp": "2026-10-12T16:47:14+02:00",
          "tree_id": "7d0bdb13ad3b51c609a683d75f11e905f8b4b925",
          "url": "https://github.com/hseeberger/tellus/commit/1cfc9e5d3904714cc4b7667ad8f631f459931d47"
        },
        "date": 1791816646752,
        "tool": "cargo",
        "benches": [
          {
            "name": "persist/no_snapshots",
            "value": 2438850,
            "range": "± 13724",
            "unit": "ns/iter"
          },
          {
            "name": "persist/snapshots",
            "value": 2448028,
            "range": "± 69404",
            "unit": "ns/iter"
          },
          {
            "name": "recover/replay",
            "value": 1056399,
            "range": "± 6745",
            "unit": "ns/iter"
          },
          {
            "name": "recover/snapshot",
            "value": 146044,
            "range": "± 1869",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "git@heikoseeberger.de",
            "name": "Heiko Seeberger",
            "username": "hseeberger"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "5faf50446f320012f5722a64c7f6749c921301a7",
          "message": "Merge pull request #107 from hseeberger/build/rust\n\nbuild(deps): bump Rust to 1.99.0",
          "timestamp": "2026-10-12T17:43:57+02:00",
          "tree_id": "34fc20f51fe0c6eb87bc54579677687115d2c437",
          "url": "https://github.com/hseeberger/tellus/commit/5faf50446f320012f5722a64c7f6749c921301a7"
        },
        "date": 1791820091803,
        "tool": "cargo",
        "benches": [
          {
            "name": "persist/no_snapshots",
            "value": 3071492,
            "range": "± 65322",
            "unit": "ns/iter"
          },
          {
            "name": "persist/snapshots",
            "value": 2883408,
            "range": "± 25897",
            "unit": "ns/iter"
          },
          {
            "name": "recover/replay",
            "value": 1248100,
            "range": "± 6199",
            "unit": "ns/iter"
          },
          {
            "name": "recover/snapshot",
            "value": 151819,
            "range": "± 2566",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "git@heikoseeberger.de",
            "name": "Heiko Seeberger",
            "username": "hseeberger"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "75be86ade0a786c469a7520ab70738daa75a729e",
          "message": "Merge pull request #106 from hseeberger/release-plz-2026-10-12T14-48-32Z\n\nchore: release",
          "timestamp": "2026-10-12T19:05:54+02:00",
          "tree_id": "0a9d57698312bfa4a661f85dfb135dd82e6eeba4",
          "url": "https://github.com/hseeberger/tellus/commit/75be86ade0a786c469a7520ab70738daa75a729e"
        },
        "date": 1791824955877,
        "tool": "cargo",
        "benches": [
          {
            "name": "persist/no_snapshots",
            "value": 2706360,
            "range": "± 79294",
            "unit": "ns/iter"
          },
          {
            "name": "persist/snapshots",
            "value": 2272922,
            "range": "± 116908",
            "unit": "ns/iter"
          },
          {
            "name": "recover/replay",
            "value": 1004630,
            "range": "± 36058",
            "unit": "ns/iter"
          },
          {
            "name": "recover/snapshot",
            "value": 131517,
            "range": "± 4677",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "git@heikoseeberger.de",
            "name": "Heiko Seeberger",
            "username": "hseeberger"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "bb08edf708fad42f2d674e754131eac1e307798d",
          "message": "Merge pull request #110 from hseeberger/dependabot/github_actions/dtolnay/rust-toolchain-e2a55d2ffb04f378e9626c28d38b36d230d1e12f\n\nci(deps): bump dtolnay/rust-toolchain from 7e38f4b43b4db5c8dd498af069a4f6196df1d067 to e2a55d2ffb04f378e9626c28d38b36d230d1e12f",
          "timestamp": "2026-10-14T21:13:48+02:00",
          "tree_id": "6bc9d01e9f9b1d89aa4c898f10b4ea3525bd26b6",
          "url": "https://github.com/hseeberger/tellus/commit/bb08edf708fad42f2d674e754131eac1e307798d"
        },
        "date": 1792005431199,
        "tool": "cargo",
        "benches": [
          {
            "name": "persist/no_snapshots",
            "value": 2073677,
            "range": "± 46656",
            "unit": "ns/iter"
          },
          {
            "name": "persist/snapshots",
            "value": 2060811,
            "range": "± 60908",
            "unit": "ns/iter"
          },
          {
            "name": "recover/replay",
            "value": 1079503,
            "range": "± 35959",
            "unit": "ns/iter"
          },
          {
            "name": "recover/snapshot",
            "value": 123531,
            "range": "± 6685",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "git@heikoseeberger.de",
            "name": "Heiko Seeberger",
            "username": "hseeberger"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "bbcc71797fd40924b72aff281237502abdf1f4c8",
          "message": "Merge pull request #109 from hseeberger/dependabot/cargo/cargo-minor-c15a9d44dc\n\nbuild(deps): bump uuid from 1.26.1 to 1.27.0 in the cargo-minor group across 1 directory",
          "timestamp": "2026-10-14T21:26:39+02:00",
          "tree_id": "f04e30fb4a7aba7b3495ed786dfb5c9fe9a8a4b8",
          "url": "https://github.com/hseeberger/tellus/commit/bbcc71797fd40924b72aff281237502abdf1f4c8"
        },
        "date": 1792006221073,
        "tool": "cargo",
        "benches": [
          {
            "name": "persist/no_snapshots",
            "value": 2432491,
            "range": "± 52428",
            "unit": "ns/iter"
          },
          {
            "name": "persist/snapshots",
            "value": 2447057,
            "range": "± 33661",
            "unit": "ns/iter"
          },
          {
            "name": "recover/replay",
            "value": 1268544,
            "range": "± 56381",
            "unit": "ns/iter"
          },
          {
            "name": "recover/snapshot",
            "value": 148072,
            "range": "± 7140",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "git@heikoseeberger.de",
            "name": "Heiko Seeberger",
            "username": "hseeberger"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "160f493ca931d0d3422eceac533435e911af065c",
          "message": "Merge pull request #116 from hseeberger/fix/rustsec-rustls\n\nfix(deps): bump rustls to 0.23.46",
          "timestamp": "2026-10-15T14:41:16+02:00",
          "tree_id": "cdae37d67396495d271a829fed3dd8375459c953",
          "url": "https://github.com/hseeberger/tellus/commit/160f493ca931d0d3422eceac533435e911af065c"
        },
        "date": 1792068284499,
        "tool": "cargo",
        "benches": [
          {
            "name": "persist/no_snapshots",
            "value": 3114699,
            "range": "± 20603",
            "unit": "ns/iter"
          },
          {
            "name": "persist/snapshots",
            "value": 3097114,
            "range": "± 28422",
            "unit": "ns/iter"
          },
          {
            "name": "recover/replay",
            "value": 1806925,
            "range": "± 11260",
            "unit": "ns/iter"
          },
          {
            "name": "recover/snapshot",
            "value": 221320,
            "range": "± 6650",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "git@heikoseeberger.de",
            "name": "Heiko Seeberger",
            "username": "hseeberger"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "2218e9ce5a93c76d726c4cd5182282019a6da812",
          "message": "Merge pull request #117 from hseeberger/chore/agents\n\nchore: use AGENTS.md instead of CLAUDE.md",
          "timestamp": "2026-10-16T19:06:55+02:00",
          "tree_id": "bd58a0b8c7c8279b2dc3f7d324c8b251ff0ba20e",
          "url": "https://github.com/hseeberger/tellus/commit/2218e9ce5a93c76d726c4cd5182282019a6da812"
        },
        "date": 1792170617576,
        "tool": "cargo",
        "benches": [
          {
            "name": "persist/no_snapshots",
            "value": 3068104,
            "range": "± 40079",
            "unit": "ns/iter"
          },
          {
            "name": "persist/snapshots",
            "value": 3060938,
            "range": "± 190255",
            "unit": "ns/iter"
          },
          {
            "name": "recover/replay",
            "value": 1817592,
            "range": "± 13205",
            "unit": "ns/iter"
          },
          {
            "name": "recover/snapshot",
            "value": 222222,
            "range": "± 9548",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "git@heikoseeberger.de",
            "name": "Heiko Seeberger",
            "username": "hseeberger"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "8fa5ed5606022b2e32e677ed201840298fbc8810",
          "message": "Merge pull request #118 from hseeberger/chore/drop-homepage\n\nchore: drop redundant homepage from manifests",
          "timestamp": "2026-10-17T09:18:28+02:00",
          "tree_id": "956a033568addc822a18a47e1bb5aba0080e3d9e",
          "url": "https://github.com/hseeberger/tellus/commit/8fa5ed5606022b2e32e677ed201840298fbc8810"
        },
        "date": 1792221729195,
        "tool": "cargo",
        "benches": [
          {
            "name": "persist/no_snapshots",
            "value": 3630510,
            "range": "± 59777",
            "unit": "ns/iter"
          },
          {
            "name": "persist/snapshots",
            "value": 3278041,
            "range": "± 16307",
            "unit": "ns/iter"
          },
          {
            "name": "recover/replay",
            "value": 1361973,
            "range": "± 4532",
            "unit": "ns/iter"
          },
          {
            "name": "recover/snapshot",
            "value": 178621,
            "range": "± 3385",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "git@heikoseeberger.de",
            "name": "Heiko Seeberger",
            "username": "hseeberger"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "195710f2acbabf6493e757497db2dc926adfd611",
          "message": "Merge pull request #120 from hseeberger/chore/dev-profile-debug\n\nbuild: shrink dev build output via debug profile settings",
          "timestamp": "2026-10-17T17:43:49+02:00",
          "tree_id": "287fd2dcd81c3030276084043f6d2b725ac7ae4b",
          "url": "https://github.com/hseeberger/tellus/commit/195710f2acbabf6493e757497db2dc926adfd611"
        },
        "date": 1792252031258,
        "tool": "cargo",
        "benches": [
          {
            "name": "persist/no_snapshots",
            "value": 3126666,
            "range": "± 28975",
            "unit": "ns/iter"
          },
          {
            "name": "persist/snapshots",
            "value": 3092791,
            "range": "± 16396",
            "unit": "ns/iter"
          },
          {
            "name": "recover/replay",
            "value": 1814454,
            "range": "± 8573",
            "unit": "ns/iter"
          },
          {
            "name": "recover/snapshot",
            "value": 220475,
            "range": "± 5589",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "git@heikoseeberger.de",
            "name": "Heiko Seeberger",
            "username": "hseeberger"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "358c3fff403d4c4a381f585281ad9a55436cf26e",
          "message": "Merge pull request #126 from tellus-rs/chore/move-to-tellus-rs\n\nchore: move repo links and code owners to tellus-rs",
          "timestamp": "2026-10-17T21:02:28+02:00",
          "tree_id": "1b0039587d76b24bbe3e99f13e2fc5fdb7d1cf67",
          "url": "https://github.com/tellus-rs/tellus/commit/358c3fff403d4c4a381f585281ad9a55436cf26e"
        },
        "date": 1792263955170,
        "tool": "cargo",
        "benches": [
          {
            "name": "persist/no_snapshots",
            "value": 3111127,
            "range": "± 5667",
            "unit": "ns/iter"
          },
          {
            "name": "persist/snapshots",
            "value": 3057397,
            "range": "± 18175",
            "unit": "ns/iter"
          },
          {
            "name": "recover/replay",
            "value": 1691523,
            "range": "± 8320",
            "unit": "ns/iter"
          },
          {
            "name": "recover/snapshot",
            "value": 213047,
            "range": "± 4312",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "git@heikoseeberger.de",
            "name": "Heiko Seeberger",
            "username": "hseeberger"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "cda2e16ac484e64b0a2437aa333692c29ce7de99",
          "message": "Merge pull request #127 from tellus-rs/ci/trusted-publishing\n\nci: publish via crates.io trusted publishing, use app client id",
          "timestamp": "2026-10-17T21:17:50+02:00",
          "tree_id": "a2d750757f546f9bd71dd683305117df86f9e3c9",
          "url": "https://github.com/tellus-rs/tellus/commit/cda2e16ac484e64b0a2437aa333692c29ce7de99"
        },
        "date": 1792264870169,
        "tool": "cargo",
        "benches": [
          {
            "name": "persist/no_snapshots",
            "value": 2129321,
            "range": "± 29031",
            "unit": "ns/iter"
          },
          {
            "name": "persist/snapshots",
            "value": 2148799,
            "range": "± 48339",
            "unit": "ns/iter"
          },
          {
            "name": "recover/replay",
            "value": 1023493,
            "range": "± 31259",
            "unit": "ns/iter"
          },
          {
            "name": "recover/snapshot",
            "value": 128510,
            "range": "± 1905",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "git@heikoseeberger.de",
            "name": "Heiko Seeberger",
            "username": "hseeberger"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "1dd5dfb8bb7ed8d2338a1a6fb3fbde822a11d6dd",
          "message": "Merge pull request #125 from tellus-rs/release-plz-2026-10-17T18-42-27Z\n\nchore: release",
          "timestamp": "2026-10-17T21:25:01+02:00",
          "tree_id": "98f5ff9b799cdfe0efac6f2159d3338fea3e0aee",
          "url": "https://github.com/tellus-rs/tellus/commit/1dd5dfb8bb7ed8d2338a1a6fb3fbde822a11d6dd"
        },
        "date": 1792265311156,
        "tool": "cargo",
        "benches": [
          {
            "name": "persist/no_snapshots",
            "value": 2446083,
            "range": "± 7987",
            "unit": "ns/iter"
          },
          {
            "name": "persist/snapshots",
            "value": 2436241,
            "range": "± 14341",
            "unit": "ns/iter"
          },
          {
            "name": "recover/replay",
            "value": 1270644,
            "range": "± 6234",
            "unit": "ns/iter"
          },
          {
            "name": "recover/snapshot",
            "value": 144382,
            "range": "± 3266",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "git@heikoseeberger.de",
            "name": "Heiko Seeberger",
            "username": "hseeberger"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "e2ce1270bdc528c00f2ed28fb83482df6775b18f",
          "message": "Merge pull request #124 from tellus-rs/dependabot/github_actions/dtolnay/rust-toolchain-683b588c487a69d01f6c6ddf40f036ff682cc2c8\n\nci(deps): bump dtolnay/rust-toolchain from e2a55d2ffb04f378e9626c28d38b36d230d1e12f to 683b588c487a69d01f6c6ddf40f036ff682cc2c8",
          "timestamp": "2026-10-17T21:49:53+02:00",
          "tree_id": "f75801721ce40c89a34e59598ddd7da8ab44cef8",
          "url": "https://github.com/tellus-rs/tellus/commit/e2ce1270bdc528c00f2ed28fb83482df6775b18f"
        },
        "date": 1792266806536,
        "tool": "cargo",
        "benches": [
          {
            "name": "persist/no_snapshots",
            "value": 3104885,
            "range": "± 12748",
            "unit": "ns/iter"
          },
          {
            "name": "persist/snapshots",
            "value": 3174550,
            "range": "± 29457",
            "unit": "ns/iter"
          },
          {
            "name": "recover/replay",
            "value": 1702958,
            "range": "± 12411",
            "unit": "ns/iter"
          },
          {
            "name": "recover/snapshot",
            "value": 216384,
            "range": "± 5001",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "git@heikoseeberger.de",
            "name": "Heiko Seeberger",
            "username": "hseeberger"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "ec9697bd21db5b02a27db35bb277dc4587772e3b",
          "message": "Merge pull request #128 from tellus-rs/docs/tellus-rs-links\n\ndocs: point links and crate homepage to tellus.rs",
          "timestamp": "2026-10-18T00:08:16+02:00",
          "tree_id": "a17c13ca7f18a129df3fd03b817587a2dfbed380",
          "url": "https://github.com/tellus-rs/tellus/commit/ec9697bd21db5b02a27db35bb277dc4587772e3b"
        },
        "date": 1792275104515,
        "tool": "cargo",
        "benches": [
          {
            "name": "persist/no_snapshots",
            "value": 2484905,
            "range": "± 11437",
            "unit": "ns/iter"
          },
          {
            "name": "persist/snapshots",
            "value": 2473569,
            "range": "± 13439",
            "unit": "ns/iter"
          },
          {
            "name": "recover/replay",
            "value": 1267334,
            "range": "± 3055",
            "unit": "ns/iter"
          },
          {
            "name": "recover/snapshot",
            "value": 151938,
            "range": "± 2307",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "git@heikoseeberger.de",
            "name": "Heiko Seeberger",
            "username": "hseeberger"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "4893ce23ffdb54aa73c08e58944e5804af81685d",
          "message": "Merge pull request #130 from tellus-rs/docs/tutorial-getting-started\n\ndocs: unify the examples into a getting started tutorial",
          "timestamp": "2026-10-18T21:37:37+03:00",
          "tree_id": "b459381be11390a1580cdda86f9297f22869828f",
          "url": "https://github.com/tellus-rs/tellus/commit/4893ce23ffdb54aa73c08e58944e5804af81685d"
        },
        "date": 1792348861515,
        "tool": "cargo",
        "benches": [
          {
            "name": "persist/no_snapshots",
            "value": 3054471,
            "range": "± 34519",
            "unit": "ns/iter"
          },
          {
            "name": "persist/snapshots",
            "value": 3021041,
            "range": "± 11259",
            "unit": "ns/iter"
          },
          {
            "name": "recover/replay",
            "value": 1697217,
            "range": "± 9821",
            "unit": "ns/iter"
          },
          {
            "name": "recover/snapshot",
            "value": 212163,
            "range": "± 5207",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "git@heikoseeberger.de",
            "name": "Heiko Seeberger",
            "username": "hseeberger"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "ad1010dcdec074cba11439711cdbcdee953baac2",
          "message": "Merge pull request #131 from tellus-rs/release-plz-2026-10-18T18-39-53Z\n\nchore: release",
          "timestamp": "2026-10-18T21:48:34+03:00",
          "tree_id": "f3d71d7c3fab43a16795c30b7571f2d633d9d32f",
          "url": "https://github.com/tellus-rs/tellus/commit/ad1010dcdec074cba11439711cdbcdee953baac2"
        },
        "date": 1792349531321,
        "tool": "cargo",
        "benches": [
          {
            "name": "persist/no_snapshots",
            "value": 2481032,
            "range": "± 36015",
            "unit": "ns/iter"
          },
          {
            "name": "persist/snapshots",
            "value": 2458749,
            "range": "± 5589",
            "unit": "ns/iter"
          },
          {
            "name": "recover/replay",
            "value": 1264844,
            "range": "± 6279",
            "unit": "ns/iter"
          },
          {
            "name": "recover/snapshot",
            "value": 149809,
            "range": "± 3637",
            "unit": "ns/iter"
          }
        ]
      }
    ]
  }
}