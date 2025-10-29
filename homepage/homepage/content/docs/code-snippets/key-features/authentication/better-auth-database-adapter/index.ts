import { startWorker } from "jazz-tools/worker";
import { co, z } from "jazz-tools";
const apiKey = process.env.JAZZ_API_KEY;

const worker1 = await startWorker({
  syncServer: `wss://cloud.jazz.tools/?key=${apiKey}`,
  accountID: process.env.WORKER_ACCOUNT_ID,
  accountSecret: process.env.WORKER_ACCOUNT_SECRET,
});

const DatabaseRoot = co.map({
  tables: co.map({
    user: co.list(
      co.map({
        name: z.string(),
        email: z.string(),
      }),
    ),
  }),
});

const db = await DatabaseRoot.loadUnique(
  "better-auth-root",
  process.env.WORKER_ACCOUNT_ID!,
  {
    resolve: {
      tables: {
        user: {
          $each: true,
        },
      },
    },
  },
);

if (db.$isLoaded) {
  console.log(db.tables.user);
}
