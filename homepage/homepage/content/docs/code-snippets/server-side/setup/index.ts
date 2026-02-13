import { co } from "jazz-tools";
const MyWorkerAccount = co.account();
type MyWorkerAccount = co.loaded<typeof MyWorkerAccount>;

/**
 * Use your email as a temporary key, or get a free
 * API Key at dashboard.jazz.tools for higher limits.
 *
 * @link https://dashboard.jazz.tools
 */
const apiKey = "you@example.com";

// #region Basic
import { startWorker } from "jazz-tools/worker";

const { worker } = await startWorker({
  AccountSchema: MyWorkerAccount,
  syncServer: `wss://cloud.jazz.tools/?key=${apiKey}`,
  accountID: process.env.JAZZ_WORKER_ACCOUNT,
  accountSecret: process.env.JAZZ_WORKER_SECRET,
});
// #endregion
