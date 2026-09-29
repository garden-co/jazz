# BandChat benchmark variant

[metadata.ts](metadata.ts) owns the wall-clock descriptions, timing boundaries
and work denominators used by the examples page. CodSpeed measures the
`walltime` suite on its macro runner through the native workload matrix in
`.github/workflows/codspeed.yml`; benchmark names carry a `band_chat_` prefix
because the page matches results by exact name.

This self-contained Rust package duplicates only the BandChat schema and query
shapes needed for measurement. It does not import the application runtime or its
fixture helpers. BandChat owns the **chat** area: opening and sending in a
members-only room, timeline pages, unread rooms, caught-up resume, and live-room
fan-out.

## Cases and former names

| Case                                          | Former case                                                               |
| --------------------------------------------- | ------------------------------------------------------------------------- |
| `band_chat_open_room[10000]`                  | `chat_open_chat[10000]` (Chat example); also covers `auth_chat_open_room` |
| `band_chat_send_100[10000]`                   | `chat_send_100[10000]` (Chat example); also covers `auth_chat_send`       |
| `band_chat_new_message_rooms_open[100]`       | `matching_write_fanout` (`crates/jazz` route subscription curve)          |
| `band_chat_timeline_second_page[4096]`        | unchanged (the 1,024 point was dropped)                                   |
| `band_chat_unread_recent_rooms[4096]`         | unchanged (the 1,024 point was dropped)                                   |
| `band_chat_caught_up_fast_resume[100, 10000]` | unchanged (the 1,000 point was dropped)                                   |

`band_chat_author_history` was dropped: StagePlan's bounded activity page
measures the same indexed, ordered, bounded read. The route subscription curve
stays in `crates/jazz/benches` for the realistic suite and its contract test,
but is no longer measured on CodSpeed; `attach_route_bindings` is covered by
Wequencer's `wequencer_open_pattern_views[100]`.

`src/membership_room.rs` (from the Chat example) opens a room's latest page as
a member and sends messages, with an outsider who must never see them.
`src/live_rooms.rs` keeps 100 live room subscriptions open and posts one
message: exactly one room wakes and unrelated rooms stay quiet.

## Timeline, unread rooms and resume

At 4,096 messages, the deterministic fixture creates 32
users, one room per 16 messages, one membership per room, and a 100-message hot
room. Each member's successive room memberships alternate unread/read so the
unread predicate selects a strict subset. The measured prepared Jazz reads cover:

- the second 25-message page of a room timeline, newest first;
- unread rooms for one member, ordered by recent activity.

It also measures a caught-up message-history resume at 100 and 10,000
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
cargo bench -p jazz-example-band-chat-benchmark --bench walltime
```
