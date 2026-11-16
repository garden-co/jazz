// Opens the .64-written store copy (fixture-data, written on .65 by the smoke) and reads alice's rows.
import { app } from "./schema.js";
import { clientAs, check, done } from "./client.js";
const alice = await clientAs("alice");
const t: any[] = await alice.db.all(app.tasks, { tier: "remote" });
check("existing .65-written store opens; alice still sees her task", t.length >= 1 && t[0].status === "done", JSON.stringify(t[0]));
done();
