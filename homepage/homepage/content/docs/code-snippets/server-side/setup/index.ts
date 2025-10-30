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
// @ts-expect-error redeclared
import { startWorker } from "jazz-tools/worker";

// @ts-expect-error redeclared
const { worker } = await startWorker({
  AccountSchema: MyWorkerAccount,
  syncServer: `wss://cloud.jazz.tools/?key=${apiKey}`,
  accountID: process.env.JAZZ_WORKER_ACCOUNT,
  accountSecret: process.env.JAZZ_WORKER_SECRET,
});
// #endregion

// #region NapiCrypto
// @ts-expect-error redeclared
import { startWorker } from "jazz-tools/worker";
import { NapiCrypto } from "jazz-tools/napi";

// @ts-expect-error redeclared
const { worker } = await startWorker({
  syncServer: `wss://cloud.jazz.tools/?key=${apiKey}`,
  accountID: process.env.JAZZ_WORKER_ACCOUNT,
  accountSecret: process.env.JAZZ_WORKER_SECRET,
  crypto: await NapiCrypto.create(),
});
// #endregion
