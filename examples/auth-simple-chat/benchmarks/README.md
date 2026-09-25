# Auth chat benchmark variant

[metadata.ts](metadata.ts) owns the wall-clock descriptions and work-unit
denominators the examples page and performance timeline read.

This self-contained Rust package models the message room shared by the auth
examples. Its read and insert policies are those of `auth-simple-chat` and
`auth-workos-chat`: the general room needs a `member` or `admin` role claim and
announcements are admin-only. `auth-betterauth-chat` uses the same table without
the role check on the general room. It does not import application runtime code.

The fixture seeds 1,000 or 10,000 messages into each of the general room and
announcements, settled before timing. The member's role claim is admitted before
timing; token verification is outside the receipt. The measured workloads are:

- `auth_chat_open_room`: the member subscribes to the general room's whole
  history in send order (`ChatPanel`'s query) until the first published result;
- `auth_chat_send`: the member sends 100 messages (10 at 10,000 messages) into
  the open room. The in-process authority runs the claim-gated insert policy
  and accepts each message, and the open room shows it before the next is sent.
  Each send's room update scales with the retained history (#2086), which is
  why the 10k case sends fewer messages; compare per-message cost.

Tests assert that a user without a role claim sees nothing and that a member's
announcement is rejected.

```sh
cargo test -p jazz-example-auth-chat-benchmark
cargo bench -p jazz-example-auth-chat-benchmark --bench walltime
```
