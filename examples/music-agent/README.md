# MusicAgent

MusicAgent is an LLM agent for a band's booking agent. You chat with it about
venues, tour dates and setlists. It streams its replies token by token, calls
tools against the workspace's booking data, keeps audio attachments (a rough
mix for the pitch), lets you regenerate a reply as a new branch, and keeps
generating on the server when you close the tab. If the server dies halfway
through a reply, the reply is marked interrupted and you can resume it.

The family is split by concern:

- `apps/nextjs/` is the full app: Next.js, Better Auth and the Astryx chat
  components with the Jazz theme.
- `apps/ts-localfirst/` is a small headless TypeScript library with the same
  transcript shape and a provider-free agent, tested against a real Jazz
  runtime.
- `benchmarks/` is a self-contained native benchmark model. It measures the
  append, range-read and materialization shapes that make an agent transcript
  different from an ordinary chat timeline.

## Run the app

```sh
pnpm install
pnpm build:ci            # builds the Jazz runtime and jazz-tools once
cd examples/music-agent/apps/nextjs
pnpm dev                 # starts Next.js and a local Jazz server
```

Open <http://127.0.0.1:3000>, create an account and the first conversation
seeds itself: a request with an attached rough mix, answered live by the agent.
Open the same account in a second browser window to watch replies stream into
both.

### Choosing the agent

| `MUSIC_AGENT_PROVIDER` | What answers                                                                                                                                            |
| ---------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `scripted` (default)   | A deterministic scripted agent. No API key, same reply every time. Replies are labelled "Scripted agent". Use it for demos and tests.                   |
| `anthropic`            | Claude, through the official `@anthropic-ai/sdk` with streaming and tool use. Needs `ANTHROPIC_API_KEY`; `ANTHROPIC_MODEL` overrides the default model. |

Both providers use the same three tools, backed by the seeded Jazz data:
`find_venues`, `check_calendar` and `draft_setlist`.

## How it works

**The browser writes, the server answers.** The browser inserts the user's turn
and any audio into Jazz itself (`insertStreaming` for the file), then calls
`POST /api/turns`. That route checks ownership, queues an assistant turn and
returns immediately; the reply is generated after the response with Next's
`after()`. The browser never receives the reply from that request. It watches
the turn's row, like every other open client.

**Streaming is a lease write.** The runner (`src/agent/runner.ts`) collects
tokens while its previous write is in flight and writes each batch in an
exclusive transaction that first checks the turn is still `streaming` under
this runner's lease:

```ts
const write = await db.exclusiveTransaction(async (tx) => {
  const turn = await tx.one(app.turns.where({ id: turnId }));
  if (turn?.runnerId !== lease.id || turn.status !== "streaming") return false;
  const end = turn.body.length;
  tx.update(
    app.turns,
    turnId,
    { heartbeatAt: new Date() },
    {
      applyDiffs: {
        body: { within: { from: end, to: end }, splices: [{ at: 0, delete: 0, insert: batch }] },
      },
    },
  );
  return true;
});
await write.wait();
```

Every subscribed client sees the reply grow. Each batch is a page-relative
splice at the end of the body, applied inside the lease transaction, so a
write carries only the new text however long the reply gets, and it commits
together with the ownership check. Tool calls are rows of their
own, written as `running` and then updated with their result (in the same
kind of lease transaction), so they show up in `ChatToolCalls` while they run.

**Durable execution.** The server writes with backend authority; permissions
let a client write only its own user turns and attachments, never an agent
reply. Queueing a reply and moving the conversation's head to it happen in one
exclusive transaction. A runner claims the turn with a lease id of its own
(`runnerId`). Everything it writes afterwards (body batches, tool calls, a
heartbeat every 3 seconds when nothing else was written, and the final
status) goes through that lease transaction, one at a time. Each commits only
while the turn still belongs to the lease and renews its heartbeat. Once a
write finds another owner, the runner aborts the model call and writes
nothing more: the authority rejects a transaction whose read changed, so a
runner that was taken over can't land a late write.
`instrumentation.ts` checks the provider and starts a sweeper when the server
boots: a `streaming` turn whose heartbeat is older than 15 seconds lost its
process, and a `queued` turn that no runner claimed within 15 seconds never
started; both are marked `interrupted` (in an exclusive transaction, so two
servers never disagree). The conversation then offers **Resume**, which claims
the turn again and continues in place, or **Regenerate**. The scripted agent is
deterministic, so resuming replays it and skips what was already written; Claude
is shown its partial reply and asked to continue.

**Branches.** Turns form a tree through `parentId`. Regenerating adds a sibling
reply under the same user turn; the conversation's `headTurnId` says which leaf
is shown, and the arrows under a reply switch between siblings. Older answers
are never overwritten.

**Audio attachments are read in ranges.** The inline player is the browser's
audio element pointed at `/api/attachments/[id]`. That route answers HTTP range
requests with a typed partial selection of the `bytes` column:

```ts
db.one(app.attachments.where({ id }).select({ payload: { from: start, to: end } }));
```

so seeking in a long recording reads only the page it needs. List views select
attachment metadata only and never load the bytes.

## Checks

```sh
pnpm --dir examples/music-agent/apps/nextjs typecheck
pnpm --dir examples/music-agent/apps/nextjs test
pnpm --dir examples/music-agent/apps/nextjs build
pnpm --dir examples/music-agent/apps/ts-localfirst test
pnpm --dir examples/music-agent/apps/ts-localfirst typecheck
cargo test -p jazz-example-music-agent-benchmark
```

The Next.js tests run the agent runner, tools and recovery against a local Jazz
server with the app's real schema and permissions, including runners racing
for one reply in one process and across two backend clients, a runner that
loses its lease, and a second user who can't read or build on the first
user's workspace. The library tests run the
same scenarios on an in-memory store and on Jazz, including a second client
watching an assistant turn grow append by append.
