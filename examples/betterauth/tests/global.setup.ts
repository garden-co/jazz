import fs from "node:fs";
import { test as setup } from "@playwright/test";
import { createWorkerAccount } from "jazz-run/createWorkerAccount";

setup("global setup", async () => {
  console.log("global setup");

  const workerAccount = await createWorkerAccount({
    name: "test",
    peer: `ws://localhost:4200`,
  });

  const accountID = workerAccount.accountID;
  const accountSecret = workerAccount.agentSecret;

  fs.appendFileSync(
    `${import.meta.dirname}/../.env`,
    `SYNC_SERVER=ws://localhost:4200\n`,
  );
  fs.appendFileSync(
    `${import.meta.dirname}/../.env`,
    `WORKER_ACCOUNT_ID=${accountID}\n`,
  );
  fs.appendFileSync(
    `${import.meta.dirname}/../.env`,
    `WORKER_ACCOUNT_SECRET=${accountSecret}\n`,
  );

  console.log("global setup done");
});
