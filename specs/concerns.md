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
