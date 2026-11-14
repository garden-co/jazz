// The stress app's backend: createJazz on port 1625, serving Jazz under
// /jazz. A core admits only its app's backend, so the app never connects to a
// sync server itself; every device is a local-first guest of this backend.
//
//   pnpm backend    (data in ./.jazz; JAZZ_IN_MEMORY=1 keeps it in memory)
import { createServer } from "node:http";
import { acceptDeviceAccounts, createJazz, memory, sqlite } from "jazz-tools/backend";
import { app } from "./schema";
import permissions from "./permissions";

const port = Number(process.env.PORT ?? 1625);
const jazz = createJazz({
  app,
  permissions,
  // No sign-in: every browser is a guest, so let guests sync.
  authenticate: acceptDeviceAccounts,
  store: process.env.JAZZ_IN_MEMORY === "1" ? memory({}) : sqlite({ path: ".jazz" }),
});
const server = createServer((req, res) =>
  jazz.nodeHandler(req, res, () => {
    res.statusCode = 404;
    res.end();
  }),
);
await jazz.attach(server);
// All interfaces, so the Android emulator (10.0.2.2) and devices reach it.
server.listen(port, "0.0.0.0", () => console.log(`Jazz backend on http://0.0.0.0:${port}/jazz`));
const stop = () => server.close(() => void jazz.close().then(() => process.exit(0)));
process.on("SIGINT", stop);
process.on("SIGTERM", stop);
