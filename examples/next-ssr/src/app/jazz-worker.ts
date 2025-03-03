import { startWorker } from "jazz-nodejs";

import { WorkerAccount } from "./schema";

let jazzWorker: WorkerAccount;

export const getJazzWorker = async () => {
  if (jazzWorker) return jazzWorker;
  const res = await startWorker({
    AccountSchema: WorkerAccount,
    accountID: process.env.JAZZ_ACCOUNT_ID,
    accountSecret: process.env.JAZZ_ACCOUNT_SECRET,
  });

  jazzWorker = res.worker;

  return res.worker;
};
