// A stock TS client (jazz-tools in Node) signing in through a backend's
// /jazz with a bearer, writing a show at tier global and reading it back.
// Usage: tsx client.ts <serverUrl> <bearer> <appId>  → prints JSON on success.
// A client is scoped by its serverUrl unless given a scope: the backend's
// app id, so it reaches <serverUrl>/apps/<appId>.
// Run it from apps/react-localfirst (copy it there), where jazz-tools and
// StagePlan's jazz.generated.ts resolve; go-cli's
// TestATypeScriptClientSyncsThroughTheGoEndpoint drives it.
import { createJazzClient } from "jazz-tools/client";
import { app } from "./jazz.generated.ts";

const [serverUrl, bearer, appId] = process.argv.slice(2);
let stored: string | null = null;
const session = await createJazzClient({
  serverUrl,
  scope: appId,
  driver: { type: "memory" },
  store: {
    async read() {
      return stored;
    },
    async update(t: (s: string | null) => string | null) {
      stored = t(stored);
    },
  },
  bearer: () => bearer,
} as never);
const snapshot = session.getSnapshot();
if (snapshot.status !== "ready") {
  console.log(JSON.stringify({ status: snapshot.status }));
  await session.close();
  process.exit(3);
}
const client = snapshot.client!;
const owner = client.session!.user!;
const show = await client.db
  .insert(app.shows, {
    name: "TS gig",
    venue: "Hall",
    date: "2026-12-06",
    doors: "20:00",
    chiefAccount: owner,
  })
  .wait({ tier: "global" });
const shows = await client.db.all(app.shows.where({ id: show.id }));
console.log(
  JSON.stringify({
    status: "ready",
    subject: client.session?.identity?.subject,
    account: owner,
    showId: show.id,
    read: shows.length,
  }),
);
await session.close();
process.exit(0);
