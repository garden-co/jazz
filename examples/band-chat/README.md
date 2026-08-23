# BandChat example family

BandChat is the deliberately small canonical Jazz chat: authenticated musicians create rooms, admit members, exchange messages offline-first, and attach bounded files. This directory groups real applications and benchmarks around the same recognizable load patterns without sharing implementation helpers.

| Variant                                            | What it demonstrates                                                                                             | Status   |
| -------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------- | -------- |
| [`apps/nextjs-betterauth`](apps/nextjs-betterauth) | Next.js, Better Auth JWT/JWKS, explicit server bootstrap, membership permissions, attachments, offline reconnect | runnable |
| [`benchmarks/typescript`](benchmarks/typescript)   | Domain-only room list, message window/materialization, and insert churn                                          | runnable |

The benchmark schema and fixture are intentionally duplicated. `room-list/materialize-24` corresponds to the room sidebar, `message-window/include-sender/materialize-40` to the selected conversation, and `message-churn/insert-32-rollback` to bursty sending.

Possible future variants include other framework/auth combinations, React Native, and a step-by-step tutorial. They are roadmap ideas, not shipped examples.
