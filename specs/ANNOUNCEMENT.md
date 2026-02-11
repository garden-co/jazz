# Jazz v2: A Local-First relational Database

Today, we’re proud to announce Jazz v2: a new direction for what we think a modern database should look like. In this blog post we’ll explore its defining features and how it differs from both traditional databases and other emerging modern databases.

*Note: if you’re using Jazz v0.x (which we’ll call “classic Jazz”) rest assured that we will keep maintaining the open-source codebase as well as the infrastructure for it. See the section at the end of this blogpost for more details on that, as well as differences between classic Jazz and Jazz 2 and what migration paths are available.*

## Looks familiar - relational semantics with a great TypeScript ORM & SQL escape hatch

- even for modern typesafe ORMs having relational semantics underneath clarifies edge cases!
- All of Jazz’s magic functionality like edit metadata and time travel can be expressed via columns and filters (basically teasing later sections here)
- We can then offer a vertically integrated ORM that makes use of all these features with a more modern, cohesive API
- SQL as an escape hatch
    - a lot of emerging databases choose custom schema and query languages
    - one more thing to learn, but semantics are often equivalent/isomorphic to SQL
    - a lot of people know SQL!
    - LLMs know SQL really well!

## Local-first & distributed - the best of embedded databases *and* cloud databases

- this is the biggest difference between Jazz and both traditional and other modern databases
- low-latency, same process like embedded databases
- but: syncs to upstream servers. Can store less data locally than in the cloud and can share database with millions of other clients
- also enables offline-first apps
- unlike “sync the whole db instance”, syncing is strictly driven by what is queried locally!

## Reactive SQL - a new default for a real-time world

- the velocity at which we update data is increasing: multi-device, multiplayer apps
- agents = every app is multiplayer, with 10x-100x more collaboration per user
- so in addition to one-shot queries, we want subscriptions for streaming consumption and reactive UI updates
- this gives us fine-grained

## Fast Row-level Security - expressive, optimized permissions

- permissions are typically implemented in backends and make up a large portion of the business logic
- typically, they are written in an imperative, hard to test way
- typically require several round-trips between db and backend
- Postgres’s RLS is expressive and powerful (use the same logic for permissions as well as queries)
    - but it is slightly arcane and requires many db connections to enforce permissions correctly for different client users
    - integration of external auth with db user is left to the backend, another source of issues (segue to next point)

## Auth & Enterprise Solved - with BetterAuth / WorkOS

- every app needs auth, even off-the-shelf solutions need to be integrated
- how can we reduce this to zero? Have the database understand web-standard auth natively
- general design: db clients connect using JWT token, either from the actual client, or a backend acting as a client

## Git in Every Row - collaboration, edit history & branching built-in

- Apps aren’t just becoming more real-time, they’re becoming more collaborative
- edit metadata and history are often added after the fact, and require a lot of manual boilerplate in code vs “just mutate data”
- programmers have discovered the ideal collaboration workflow with git branches - but this is even more work to implement yourself. A modern database should anticipate this as the default way to edit data as multiple users & devices
- rich text editing

## Fluid Migrations & Envs - tools for fast-moving teams and agents, shipping confidently

- heterogenous client app versions

## Blobs, Files, Streams and Media - a built-in CDN for static and dynamic data

## The perfect glue between your apps and services - JS/TS in browser and server runtimes, React, React Native & Expo, Rust, Go, Swift, Kotlin, SQL over HTTP, Webhooks

- export to external databases
    - sync to external systems (only one-way)

## Performance

## Per-Column End-to-End Encryption - for the strongest privacy and security guarantees

## Opt-in transactions - granularly trade off latency and availability with consistency

## What have we learned from classic Jazz and how the way people are building apps changed over the last three years

SEPARATE POST? -> docs page?
- for each section have a link to somewhere that explains the learnings and design decisions about that aspect in terms of jazz1 -> jazz2

## How we’ll maintain classic jazz & how we’ll help classic jazz adopters migrate to jazz2

- devs owned not just by users but by devs (visibility, debugging, data maintenance)
- simplicity & more powerful
- performance, indexing etc



Open questions:

- narrow scope for use-cases at first
    - framework/db/library for local first apps

- very important: batteries included (kinda mentioned across points here)

- list the kind of apps we want to support
    - example scissor: ecommerce? -> implies SSR, global txs
    - simple collaborative SaaS: no such constraints, better immediate fit

- example apps
    - way more affordable to build complex, polished examples!!
    - show these in the post!!
        - looks good, fast, cool features

- demostrating performance and real world scale
    - example apps with lots of data
    - tradtional db benchmarks
    - per devtools to show off concrete examples

- how do we talk about the SQL interface
    - compile target
    - escape hatch
    - other query builders
    - power users & open-ended LLMs
        - validate assumptions, might not be good with non-postgres dialects
        - important: clear errors! -> education opportunity
        - people expect a lot of SQL features (like fns, etc)
    - counterpoint:
        - only having tightly scoped DSL forces everyone into what we actually support
    - injection and missing typesafety bc it's just a string
    - "SQL first" immediately invites comparison to mature general purpose dbs
        - feature parity
        - traditional replication like cockroachdb

- perception of existing community
    - it's a breaking change on the data level!
    - e2ee is most beloved feature (?)
    - e2ee makes automatic migration tricky
    - how to transition gentle
        - link to old docs & branch
        - separate discord channels
    - actually don't call it "jazz 2"
        - it shouldn't feel like a new product, but "just" like a big breaking change
        - argument that we intentionally called it 0.x so far
    - separate repo for classic jazz
    - good jazz1 support is actually vital for keeping goodwill and getting people to migrate
        - how to make it easier
            - scale down worldwide coverage etc
            - focus on reducing complexity

- permissions
    - DX around optimistic updates is crucial