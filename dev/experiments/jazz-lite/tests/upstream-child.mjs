// Runs the upstream Jazz server in its own process so proxy memory can be measured apart from it.
import { startLocalJazzServer } from "jazz-tools/testing";

const options = JSON.parse(process.argv[2]);
const handle = await startLocalJazzServer(options);
process.send({
  appId: handle.appId,
  url: handle.url,
  port: handle.port,
  adminSecret: handle.adminSecret,
  backendSecret: handle.backendSecret,
  pid: process.pid,
});
process.on("message", async (message) => {
  if (message === "stop") {
    await handle.stop();
    process.exit(0);
  }
});
