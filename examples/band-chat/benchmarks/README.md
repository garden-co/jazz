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

| Case                                          | Former case                                                           |
| --------------------------------------------- | --------------------------------------------------------------------- |
| `band_chat_open_room[10000]`                  | `chat_open_chat[10000]` (Chat example)                                |
| `band_chat_send_100[10000]`                   | `chat_send_100[10000]` (Chat example)                                 |
| `band_chat_post_announcements[10000]`         | `auth_chat_send[10000]` and `auth_chat_open_room` (auth chat example) |
| `band_chat_new_message_rooms_open[100]`       | `matching_write_fanout` (`crates/jazz` route subscription curve)      |
| `band_chat_timeline_second_page[4096]`        | unchanged (the 1,024 point was dropped)                               |
| `band_chat_unread_recent_rooms[4096]`         | unchanged (the 1,024 point was dropped)                               |
| `band_chat_caught_up_fast_resume[100, 10000]` | unchanged (the 1,000 point was dropped)                               |

`band_chat_author_history` was dropped: StagePlan's bounded activity page
measures the same indexed, ordered, bounded read. The route subscription curve
stays in `crates/jazz/benches` for the realistic suite and its contract test,
but is no longer measured on CodSpeed; `attach_route_bindings` is covered by
Wequencer's `wequencer_open_pattern_views[100]`.

`src/membership_room.rs` (from the Chat example) opens a room's latest page as
a member and sends messages, with an outsider who must never see them.
`src/live_rooms.rs` keeps 100 live room subscriptions open and posts one
message: exactly one room wakes and unrelated rooms stay quiet.
`src/announcements.rs` (from the auth chat example, modelled in the benchmark
only: the app has no claim-gated room) is the one case with session-claim
policies and an unbounded view: reading needs a `member` or `admin` role
claim, posting announcements needs `admin`, and the admin's open view is the
room's whole history, so every post updates a 10,000-message view (#2086).
The auth chat room opening itself (`auth_chat_open_room`) happens untimed in
its fixture; `band_chat_open_room` measures opening a policy-protected room.

## A band after years of use

`src/deep_room.rs` measures BandChat where real chats spend their time: deep
in one room's history, in a room list of a hundred rooms, and on read state.
Its schema and policies mirror the app's `schema.ts` and `permissions.ts`: the
room list reads each room's newest message instead of a `lastActivityAt` that
members bump, markers are readable by co-members (check marks), and every
marker move appends a `readProgress` row (read dates).

The fixture is a power law, not a uniform spread: 200 members and 1,000 rooms,
one with 100,000 messages and 50 members, ten with 10,000 and 989 with 10–200
(303,695 messages). The busy rooms carry reactions and a read journal; the
reader belongs to 100 rooms and has five unread messages in the deep room.

| Case                                         | What the user does                                  |
| -------------------------------------------- | --------------------------------------------------- |
| `band_chat_open_deep_room[100000]`           | Opens the deep room: newest 50, senders, reactions  |
| `band_chat_scroll_back_deep[100000]`         | The 50 before a cursor halfway down                 |
| `band_chat_jump_to_message[100000]`          | 25 up to a message a quarter in, and 25 after it    |
| `band_chat_search_room[100000]`              | Newest 50 messages containing a word, one read      |
| `band_chat_live_window_new_message[100000]`  | A bandmate's message reaches the open deep room     |
| `band_chat_inbox_open[100]`                  | The room list: 100 rooms, newest message, marker    |
| `band_chat_inbox_new_message[100]`           | A message reaches the open room list                |
| `band_chat_unread_count_deep[100000]`        | Capped count after the marker: five unread          |
| `band_chat_unread_count_new_message[100000]` | The live count goes up                              |
| `band_chat_marker_move_fanout[50]`           | One member reads; 50 open views see the marker move |
| `band_chat_read_by_sheet[50]`                | "Read by" for a recent message, one read            |

Every page and count here is bounded (50, 25 + 25, limit 1, cap 100), so none
of them should cost the room's or the table's size.

These cases are the `nightly` bench target, measured by the nightly CodSpeed
run on main rather than on every merge: each case seeds the band (17 s on an
M-series laptop) and the reads take seconds today. Run them locally with
`cargo bench -p jazz-example-band-chat-benchmark --bench nightly`.
`tests/deep_room.rs` checks every case's result on a miniature of the same
shape.

### Memory

`band-chat-memory-receipt` counts heap bytes at the global allocator while
each read runs: the peak, what the open view holds, and what is left once the
view is dropped and the engine has finalized it. Process RSS cannot answer
this per read, because the allocator keeps freed pages.

```sh
cargo run --release -p jazz-example-band-chat-benchmark --bin band-chat-memory-receipt -- 100000
```

At 100,000 messages the seeded store itself holds 1,253 MB (in-memory
storage, so that is the whole database):

| Read                | Rows | Peak MB | Held open MB | After close MB |
| ------------------- | ---- | ------- | ------------ | -------------- |
| Open the room       | 50   | 3,715   | 1,768        | 7              |
| Scroll back         | 50   | 3,600   | 1,663        | 3              |
| Jump to a message   | 50   | 5,145   | 1,783        | 5              |
| Search the room     | 50   | 1,732   | 1            | 1              |
| Unread count        | 5    | 3,235   | 1,309        | 1,309          |
| Room list           | 100  | 5,004   | 2,377        | 1,736          |
| Read by             | 20   | 1,280   | 2            | 2              |
| Open the room again | 50   | 3,707   | 822          | 4              |

A 50-message page needs about three times the database while it opens and
keeps more than the database while it stays open. The unread count and the
room list keep most of what they held after they close.

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
