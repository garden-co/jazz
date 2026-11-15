// Only checks the backend starts and a ticket WITHOUT a kind round-trips (writes with a kind fail on .64: jazz#5621).
import { app } from "./schema.js";
import { account, clientAs, check, done } from "./client.js";
const aId = await account("alice"); const alice = await clientAs("alice");
await alice.db.insert(app.tickets, { title: "no kind", owner_id: aId, kind: null } as any).wait({ tier: "global" });
check("ticket without kind round-trips", (await alice.db.all(app.tickets, { tier: "remote" })).length === 1);
done();
