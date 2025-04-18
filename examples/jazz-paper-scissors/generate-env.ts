import * as fs from "fs";
import { createWorkerAccount } from "jazz-run/createWorkerAccount";

if (fs.existsSync(".env")) {
  process.exit(0);
}

const account = await createWorkerAccount({
  name: "jazz-paper-scissors-worker",
  peer: "wss://cloud.jazz.tools/?key=jazz-paper-scissors@garden.co",
});

fs.writeFileSync(
  ".env",
  `
VITE_JAZZ_WORKER_ACCOUNT=${account.accountID}
JAZZ_WORKER_SECRET=${account.agentSecret}
`,
);

process.exit(0);
