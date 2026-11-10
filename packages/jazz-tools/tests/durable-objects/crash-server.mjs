// Dedicated process group owned by test-crash.mjs, never an existing server.
import { createServerHarness } from "./server-runtime.mjs";
process.env.JAZZ_DO_PACKAGED = "1";
const [appId, secret, persist, port] = process.argv.slice(2);
const mf = createServerHarness(appId, secret, {
  durableObjectsPersist: persist,
  port: Number(port),
});
process.send({ url: String(await mf.ready).replace(/\/$/, "") });
process.on("message", async (message) => {
  if (message !== "stop") return;
  await mf.dispose();
  process.disconnect();
});
