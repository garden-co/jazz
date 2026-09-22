# BandChat benchmark variant

This self-contained Rust package duplicates only the BandChat schema and query
shapes needed for measurement. It does not import the application runtime or its
fixture helpers.

At two scales (1,024 and 4,096 messages), the deterministic fixture creates 32
users, one room per 16 messages, one membership per room, and a 100-message hot
room. Each member's successive room memberships alternate unread/read so the
unread predicate selects a strict subset. The measured prepared Jazz reads cover:

- the second 25-message page of a room timeline, newest first;
- unread rooms for one member, ordered by recent activity;
- one author's message history, newest first.

It also measures a caught-up message-history resume at 100, 1,000, and 10,000
messages. The fixture learns the peer's fast-resume cursor from an actual
settled publication. Every measured resume must therefore emit no reset,
membership transition, or version payload. This is the performance receipt for
#2136: any remaining scale dependence is server-side work, not replayed data.

Database opening, schema compilation, seeding, local-durability waits, and query
preparation occur before each Divan measured closure. Only the read and returned
row count are measured and black-boxed. Tests separately assert exact result
cardinality, pagination, filtering, and order.

```sh
cargo test -p jazz-example-band-chat-benchmark
cargo bench -p jazz-example-band-chat-benchmark --bench loads
cargo bench -p jazz-example-band-chat-benchmark --bench fast_resume
```
