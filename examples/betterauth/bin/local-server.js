import fs from "node:fs";
import { createWorkerAccount } from "jazz-run/createWorkerAccount";
import { startSyncServer } from "jazz-run/startSyncServer";

const syncServer = await startSyncServer({
  port: "4200",
  inMemory: true,
  db: "betterauth.db",
});

const workerAccount = await createWorkerAccount({
  name: "test",
  peer: `ws://localhost:4200`,
});

const accountID = workerAccount.accountID;
const accountSecret = workerAccount.agentSecret;

fs.writeFileSync(
  `${import.meta.dirname}/../.env`,
  `SYNC_SERVER=ws://localhost:4200\nWORKER_ACCOUNT_ID=${accountID}\nWORKER_ACCOUNT_SECRET=${accountSecret}\n`,
);

console.log("Sync server started on port " + syncServer.address().port);
