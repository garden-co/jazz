// The device app's backend, as an app ships it: createJazz in front of the
// local Core harness, serving sync under /jazz. Devices are local-first
// guests, so `authenticate` lets every guest in and names no user. The host
// driver starts it after the Core harness and passes its URL to the device;
// the device never talks to Core directly (a core admits only its backend).
import { createServer } from "node:http";
import { acceptDeviceAccounts, createJazz, remote } from "jazz-tools/backend";
import { testApp } from "jazz-tools/testing";
import { schemaSource } from "../src/scope-fixture.ts";

const coreUrl = process.env.JAZZ_DEVICE_CORE_URL;
const appId = process.env.JAZZ_DEVICE_APP_ID;
const secret = process.env.JAZZ_DEVICE_ADMIN_SECRET;
if (!coreUrl || !appId || !secret) {
  throw new Error(
    "device backend needs JAZZ_DEVICE_CORE_URL, JAZZ_DEVICE_APP_ID and JAZZ_DEVICE_ADMIN_SECRET",
  );
}

// createJazz takes the app as its bindings give it and the permissions as
// `jazz.permissions.json`. The device attaches the fixture's own schema, so
// both are derived from it here rather than written twice: the backend then
// serves exactly the schema version the device opens. Only the shapes the
// fixture uses are translated; anything else stops the harness by name.
const COLUMN_TYPES = { Text: "string", Boolean: "boolean", Integer: "int" };

function columnType({ column_type: { type }, nullable, name }) {
  const spelled = COLUMN_TYPES[type];
  if (!spelled) throw new Error(`device backend: fixture column ${name} has type ${type}`);
  return nullable ? `${spelled}?` : spelled;
}

function policy(expr, where) {
  if (expr === null) return null;
  if (expr.type === "True") return "true";
  if (
    expr.type === "Cmp" &&
    expr.op === "Eq" &&
    expr.column === "$createdBy.account" &&
    expr.value?.type === "SessionRef" &&
    expr.value.path?.length === 1 &&
    expr.value.path[0] === "user"
  ) {
    return "$createdBy.account = session.user";
  }
  throw new Error(`device backend: fixture ${where} policy ${JSON.stringify(expr)}`);
}

/** One operation's rule: its `using`, or for an insert its `with_check`. */
function rule(operation, { using, with_check }, where) {
  const read = policy(using, `${where}.${operation}`);
  const check = policy(with_check, `${where}.${operation}`);
  if (read !== null && check !== null && read !== check) {
    throw new Error(`device backend: fixture ${where}.${operation} checks differ`);
  }
  return read ?? check;
}

function appAndPermissions(source) {
  const { tables } = JSON.parse(source);
  const app = { format: 1, tables: {} };
  const permissions = { format: 1, tables: {} };
  for (const [name, { columns, policies }] of Object.entries(tables)) {
    app.tables[name] = {
      columns: Object.fromEntries(columns.map((column) => [column.name, columnType(column)])),
    };
    if (!policies) continue;
    permissions.tables[name] = Object.fromEntries(
      ["select", "insert", "update", "delete"]
        .filter((operation) => policies[operation])
        .map((operation) => [
          operation === "select" ? "read" : operation,
          rule(operation, policies[operation], name),
        ]),
    );
  }
  return { app: testApp(app), permissions };
}

const { app, permissions } = appAndPermissions(schemaSource);
const jazz = await createJazz({
  app,
  permissions,
  store: remote({ url: coreUrl, appId, secret }),
  // Without it, createJazz refuses local-first guests.
  authenticate: acceptDeviceAccounts,
});
const server = createServer((req, res) =>
  jazz.nodeHandler(req, res, () => {
    res.statusCode = 404;
    res.end();
  }),
);
await jazz.attach(server);
// Loopback only: the iOS simulator shares the host network, and the Android
// emulator's 10.0.2.2 maps to the host's 127.0.0.1.
server.listen(0, "127.0.0.1", () => {
  const { port } = server.address();
  console.log(`JAZZ_RN_DEVICE_BACKEND ${JSON.stringify({ port, basePath: jazz.basePath })}`);
});
const stop = () => server.close(() => process.exit(0));
process.once("SIGTERM", stop);
process.once("SIGINT", stop);
