# Indexed point-join read receipt (2026-09-24)

## Why this change

A one-shot query pinned to one issue still scanned the entire `issueTags`
junction source. On a 10,000-issue SaaS fixture it returned one row but spent
tens of milliseconds executing a Groove graph. The query already proves that
every matching junction row has `issue = <bound issue id>`. The first-result
planner now uses the junction's existing foreign-key index for that exact
prefix. The normal graph still evaluates joins, read policies, and deletions.
Retained subscriptions keep their existing live source path.

## Matched public-API receipt

The direct parent is `387ff9c97` (PR #3317). The only engine difference in
the candidate is the point-join access path. Both binaries used the same
formatted `s1_saas` receipt source and `perf` profile. Parent SHA-256:
`8ef3346a088696cc9baa37e6fa66dafda2d9c2141518e65a053b3e6fffff7b7c`;
candidate SHA-256:
`48f62e771f1271c9f499a35badf751487f321b48aeb0ee9b528f47c7853d86fd`.

The public schema and query are built with the public API. One organization
has 10,000 issues and 20,170 issue-tag links, with an explicit allow-all SELECT
policy. Batches are accepted at Global durability; the fixture asserts their
fate and checks every query result against an independent oracle. The measured
point query filters the issue ID, joins `issueTags` on `issue`, filters one tag,
and includes its project. It returns exactly one issue. This is the first
SELECT after seeding on each new runtime; the RocksDB/OS cache is warm from
seeding. Each process runs seven reads; “warm” is the median of samples 1–6.
Two processes per arm ran candidate/parent/parent/candidate. The table reports
Global `Db::all_for_identity` time, with query preparation measured separately.

| Case                                                   |   Parent, two runs | Candidate, two runs |       Gain |
| ------------------------------------------------------ | -----------------: | ------------------: | ---------: |
| No deletions: prepare + first SELECT                   | 67.490 / 70.542 ms |    7.414 / 6.305 ms |  9.1–11.2× |
| No deletions: first SELECT only                        | 64.633 / 68.099 ms |    4.771 / 3.779 ms | 13.5–18.0× |
| No deletions: warm SELECT median                       | 60.887 / 59.897 ms |    0.741 / 0.689 ms | 82.2–86.9× |
| 10,000 unrelated deleted links: prepare + first SELECT | 55.591 / 57.036 ms |  13.030 / 13.553 ms |   4.2–4.3× |
| 10,000 unrelated deleted links: first SELECT only      | 53.102 / 54.572 ms |  10.778 / 10.842 ms |   4.9–5.0× |
| 10,000 unrelated deleted links: warm SELECT median     | 47.736 / 46.195 ms |    7.505 / 7.653 ms |   6.0–6.4× |

The no-deletion Local `read_profiled` execution median dropped from
38.9–40.8 ms to 0.56–0.58 ms. With 10,000 unrelated deletions it dropped
from 48.7–49.6 ms to 12.9–13.0 ms. The remaining deletion-register scan is
therefore still material. The ordinary `q1` filter/include and broader
project/state/tag join (`q2_join`) are unchanged within run variation; this
receipt does not claim a general join or all-SELECT win.

The timing policy is deliberately simple (`SELECT true` on every table). A
separate public-API regression exercises a real title-based row policy with a
hidden issue, matching and nonmatching links, deletion, and reinsertion. The
receipt does not measure network delivery, a cold reopen, or a large local
ahead overlay. It measures one-shot reads over globally accepted rows.

Raw JSONL receipts, including seed, deletion, preparation, Global reads, and
Local phase breakdowns: [parent no-deletion run 1](receipts/s1-point-join-20260924/jazz-s1-final-parent-nodelete-r1.jsonl),
[run 2](receipts/s1-point-join-20260924/jazz-s1-final-parent-nodelete-r2.jsonl),
[candidate no-deletion run 1](receipts/s1-point-join-20260924/jazz-s1-final-candidate-nodelete-r1.jsonl),
[run 2](receipts/s1-point-join-20260924/jazz-s1-final-candidate-nodelete-r2.jsonl),
[parent deleted run 1](receipts/s1-point-join-20260924/jazz-s1-final-parent-deleted-r1.jsonl),
[run 2](receipts/s1-point-join-20260924/jazz-s1-final-parent-deleted-r2.jsonl),
[candidate deleted run 1](receipts/s1-point-join-20260924/jazz-s1-final-candidate-deleted-r1.jsonl),
[run 2](receipts/s1-point-join-20260924/jazz-s1-final-candidate-deleted-r2.jsonl).

Reproduce with `JAZZ_S1_READ_RECEIPT=1 JAZZ_S1_READ_POLICY=1
JAZZ_S1_ORGS=1 JAZZ_S1_ISSUES_PER_ORG=10000 cargo bench --profile perf -p
jazz-sim --bench s1_saas`; set `JAZZ_S1_UNRELATED_DELETIONS=10000` for the
aged-data case. The benchmark emits JSONL with its own phase breakdown.

Tooling friction: rebuilding the optimized `s1_saas` binary after each source
change took longer than seeding and timing; an isolated reusable Cargo target
would also prevent cross-worktree binary reuse.
