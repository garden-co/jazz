# Policy churn and reconnect receipt — 2026-08-05

This receipt exercises byte-wire subscriber resume after authorization changes
while the reader is disconnected. It extends
`realistic_phase1/r13_permission_filtered_resume` with six independently gated
cases:

- unchanged recursive membership;
- one newly granted document;
- one revoked document;
- one grant and one revoke together;
- process-local claim revocation; and
- process-local claim restoration.

Every lane opens a writer, history-complete server, and partial reader over
`MemoryStorage`; performs the initial sync; detaches the reader while retaining
the server-side `ResumeCursor`; applies the authorization change; and reconnects
through the byte-wire transport adapter. The timer starts immediately before
the reader announces its resumed subscription and ends after the server and
reader settle the catch-up. Fixture creation, initial sync, and disconnected
authorization mutation are outside the measured interval.

Run the matrix with:

```sh
cargo bench -p jazz --bench realistic_phase1 -- \
  r13_permission_filtered_resume --noplot
```

## Correctness gates

The benchmark requires exact public subscription events:

| Case             |                   Added or updated |                           Removed |
| ---------------- | ---------------------------------: | --------------------------------: |
| unchanged        |     the same two visible documents |                              none |
| grant only       | exactly the newly granted document |                              none |
| revoke only      |                               none |      exactly the revoked document |
| grant and revoke | exactly the newly granted document |      exactly the revoked document |
| claim revoke     |                               none | both previously visible documents |
| claim restore    |       both newly visible documents |                              none |

The unchanged event is reset-framed and therefore reports the two retained rows
as updates. This does not imply that their bodies are retransmitted: its wire
response is only 61 bytes.

`Db::read` is deliberately not an oracle here. It is a local-preview API and
may retain row bodies after upstream membership is revoked. The authoritative
security contract is the subscription result-set transition.

## Initial local result

One full run on the quiet local development box (load average
`0.07 / 0.11 / 0.08` immediately before the run) produced:

| Case             | Reconnect median | Resume response | Initial response | Added | Updated | Removed |
| ---------------- | ---------------: | --------------: | ---------------: | ----: | ------: | ------: |
| unchanged        |         1.864 ms |            61 B |            856 B |     0 |       2 |       0 |
| grant only       |         2.201 ms |         1,256 B |            856 B |     1 |       0 |       0 |
| revoke only      |         1.963 ms |           516 B |            856 B |     0 |       0 |       1 |
| grant and revoke |         2.105 ms |           925 B |            856 B |     1 |       0 |       1 |
| claim revoke     |         0.728 ms |           213 B |            824 B |     0 |       0 |       2 |
| claim restore    |         0.888 ms |           824 B |             61 B |     2 |       0 |       0 |

The unchanged control retains a compact response at about 7% of its initial
snapshot. Revokes are also smaller than the initial response because row-member
removals are self-sufficient.

Grant responses can equal or exceed the reader's prior initial snapshot. A row
that existed before the cursor but was not authorized was never shipped to that
reader, so restoration must carry its body despite its old global sequence. In
the recursive grant-only lane, the resulting three-row authoritative response
is 1,256 bytes versus the prior two-row 856-byte snapshot. This is required
correctness work, not resume amplification over an equivalent snapshot.

These are single-process, single-threaded, network-free measurements. They are
useful for comparing protocol paths and detecting accidental full rehydrate;
they are not an end-user reconnect latency claim.
