const apiKey = "you@example.com";

// #region NapiCrypto
import { startWorker } from "jazz-tools/worker";
import { NapiCrypto } from "jazz-tools/napi";

const { worker } = await startWorker({
  syncServer: `wss://cloud.jazz.tools/?key=${apiKey}`,
  accountID: process.env.JAZZ_WORKER_ACCOUNT,
  accountSecret: process.env.JAZZ_WORKER_SECRET,
  crypto: await NapiCrypto.create(),
});
// #endregion
