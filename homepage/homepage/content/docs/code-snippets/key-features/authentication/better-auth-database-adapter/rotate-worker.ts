import { Account, Group, co } from "jazz-tools";
import { startWorker } from "jazz-tools/worker";
const apiKey = process.env.JAZZ_API_KEY;

// Start the main worker and fetch database reference
const { worker } = await startWorker({
  syncServer: `wss://cloud.jazz.tools/?key=${apiKey}`,
  accountID: process.env.WORKER_ACCOUNT_ID,
  accountSecret: process.env.WORKER_ACCOUNT_SECRET,
});

const DatabaseRoot = co.map({
  group: Group,
  tables: co.map({}),
});

const db = await DatabaseRoot.loadUnique(
  "better-auth-root",
  process.env.WORKER_ACCOUNT_ID!,
  {
    loadAs: worker,
    resolve: {
      group: true,
      tables: true,
    },
  },
);

// Load the new worker account
const newWorkerRef = await co
  .account()
  .load(process.env.NEW_WORKER_ACCOUNT_ID!);

if (db.$isLoaded && newWorkerRef.$isLoaded) {
  // Add the new worker to the group as admin
  db.group.addMember(newWorkerRef, "admin");
  await db.group.$jazz.waitForSync();

  // Now the new worker can access the tables
  const { worker: newWorker } = await startWorker({
    syncServer: `wss://cloud.jazz.tools/?key=${apiKey}`,
    accountID: process.env.NEW_WORKER_ACCOUNT_ID,
    accountSecret: process.env.NEW_WORKER_ACCOUNT_SECRET,
  });

  // Create the database root on the new worker with the same group's and tables' references
  await DatabaseRoot.upsertUnique({
    unique: "better-auth-root",
    value: {
      group: db.group,
      tables: db.tables,
    },
    owner: newWorker,
  });

  // Now the new worker can be used for the Database Adapter.

  // Don't forget to remove the old worker from the group
  db.group.removeMember(worker);
}
