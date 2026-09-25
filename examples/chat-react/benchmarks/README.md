# Chat benchmark variant

[metadata.ts](metadata.ts) owns the wall-clock descriptions and work-unit
denominators the examples page and performance timeline read.

This self-contained Rust package duplicates only the `chat-react` schema and
policies that opening a chat and sending a message touch. It does not import the
application runtime or its fixture helpers.

The fixture creates 32 users with profiles, 64 chats (odd chats public), four
members per chat and 1,000 or 10,000 messages, a quarter of them in the private
chat the reader opens. All seeded rows are settled by the in-process authority
before timing. The measured workloads are:

- `chat_open_chat`: a member subscribes to the newest 21 messages of a private
  chat with their senders, through the membership read policy, until the first
  published page (`ChatView`'s query);
- `chat_send_100`: a member sends 100 messages into the open chat. The
  in-process authority runs the insert policy (member of the chat, sending as
  their own profile) and accepts each message, and the open page shows it
  before the next is sent.

Reactions, canvases and network transport are outside these receipts. Tests
assert page cardinality, that a non-member sees nothing and that a non-member's
message is rejected.

```sh
cargo test -p jazz-example-chat-benchmark
cargo bench -p jazz-example-chat-benchmark --bench walltime
```
