// #region Basic
import { startWorker } from "jazz-tools/worker";

export const jazzServer = await startWorker({
  syncServer: "wss://cloud.jazz.tools/?key=your-api-key",
  accountID: process.env.JAZZ_WORKER_ACCOUNT,
  accountSecret: process.env.JAZZ_WORKER_SECRET,
});
// #endregion
