import { startWorker } from "jazz-tools/worker";

export const jazzServerAccount = await startWorker({
  syncServer: "wss://cloud.jazz.tools/?key=jazz-paper-scissors@garden.co ",
});
