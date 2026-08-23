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
response remains compact.

`Db::read` is deliberately not an oracle here. It is a local-preview API and
may retain row bodies after upstream membership is revoked. The authoritative
security contract is the subscription result-set transition.

## Refreshed local result

One refreshed full run after restacking onto the 2026-08-06 integration head
produced:

| Case             | Reconnect median | Resume response | Initial response | Added | Updated | Removed |
| ---------------- | ---------------: | --------------: | ---------------: | ----: | ------: | ------: |
| unchanged        |         2.003 ms |            62 B |            895 B |     0 |       2 |       0 |
| grant only       |         2.352 ms |         1,314 B |            895 B |     1 |       0 |       0 |
| revoke only      |         2.064 ms |           555 B |            895 B |     0 |       0 |       1 |
| grant and revoke |         2.225 ms |           983 B |            895 B |     1 |       0 |       1 |
| claim revoke     |         0.744 ms |           252 B |            863 B |     0 |       0 |       2 |
| claim restore    |         0.912 ms |           863 B |             62 B |     2 |       0 |       0 |

The unchanged control retains a compact response at about 7% of its initial
snapshot. Revokes are also smaller than the initial response because row-member
removals are self-sufficient.

Grant responses can equal or exceed the reader's prior initial snapshot. A row
that existed before the cursor but was not authorized was never shipped to that
reader, so restoration must carry its body despite its old global timestamp. In
the recursive grant-only lane, the resulting three-row authoritative response
is 1,314 bytes versus the prior two-row 895-byte snapshot. This is required
correctness work, not resume amplification over an equivalent snapshot.

These are single-process, single-threaded, network-free measurements. They are
useful for comparing protocol paths and detecting accidental full rehydrate;
they are not an end-user reconnect latency claim.
