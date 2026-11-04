// The device app's backend, as an app ships it: createJazz in front of the
// local Core harness, serving sync under /jazz. Devices are local-first
// guests, so there is no `authenticate`. The host driver starts it after the
// Core harness and passes its URL to the device; the device never talks to
// Core directly (a core admits only its backend).
import { createServer } from "node:http";
import { createJazz, remote } from "jazz-tools/backend";
import { schemaSource } from "../src/scope-fixture.ts";

const coreUrl = process.env.JAZZ_DEVICE_CORE_URL;
const appId = process.env.JAZZ_DEVICE_APP_ID;
const secret = process.env.JAZZ_DEVICE_ADMIN_SECRET;
if (!coreUrl || !appId || !secret) {
  throw new Error(
    "device backend needs JAZZ_DEVICE_CORE_URL, JAZZ_DEVICE_APP_ID and JAZZ_DEVICE_ADMIN_SECRET",
  );
}

/** The fixture's schema carries its policies; createJazz takes them apart. */
function appAndPermissions(source) {
  const { tables } = JSON.parse(source);
  const app = {};
  const permissions = {};
  for (const [name, { policies, ...table }] of Object.entries(tables)) {
    app[name] = table;
    if (policies) permissions[name] = policies;
  }
  return { app, permissions };
}

const { app, permissions } = appAndPermissions(schemaSource);
const jazz = await createJazz({
  app,
  permissions,
  store: remote({ url: coreUrl, appId, secret }),
});
const server = createServer((req, res) =>
  jazz.nodeHandler(req, res, () => {
    res.statusCode = 404;
    res.end();
  }),
);
await jazz.attach(server);
// Every interface: the Android emulator reaches the host through 10.0.2.2.
server.listen(0, "0.0.0.0", () => {
  const { port } = server.address();
  console.log(`JAZZ_RN_DEVICE_BACKEND ${JSON.stringify({ port, basePath: jazz.basePath })}`);
});
const stop = () => server.close(() => process.exit(0));
process.once("SIGTERM", stop);
process.once("SIGINT", stop);
