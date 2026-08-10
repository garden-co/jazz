import { JazzClient, loadWasmModule } from "../../../packages/jazz-tools/dist/runtime/client.js";
import { NativeRuntimeAdapter } from "../../../packages/jazz-tools/dist/runtime/native-runtime/native-runtime-adapter.js";
import { deterministicRuntimeBytes } from "../../../packages/jazz-tools/dist/runtime/runtime-identity.js";
import { app } from "../schema.ts";

const appId = process.env.JAZZ_APP_ID ?? "00000000-0000-0000-0000-000000000002";
const serverUrl = process.env.JAZZ_SERVER_URL ?? "http://127.0.0.1:1625";
const expectedTitle = process.env.JAZZ_E2E_EXPECTED_TITLE ?? "offline-seed";
const insertedTitle = process.env.JAZZ_E2E_INSERT_TITLE ?? "remote-seed";
const adminSecret = process.env.JAZZ_ADMIN_SECRET ?? "jazz-rn-e2e-admin";

function titles(rows) {
  return rows.flatMap((row) => {
    const title = row.values[0];
    return title?.type === "Text" ? [title.value] : [];
  });
}

async function waitForTitles(client, expectedTitles, timeoutMs = 15_000) {
  const deadline = Date.now() + timeoutMs;
  let latestRows = [];

  while (Date.now() < deadline) {
    latestRows = await client.query(JSON.stringify({ table: "todos" }), { tier: "local" });
    const latestTitles = titles(latestRows);
    if (expectedTitles.every((title) => latestTitles.includes(title))) {
      return latestRows;
    }
    await new Promise((resolve) => setTimeout(resolve, 25));
  }

  throw new Error(
    `Timed out waiting for ${expectedTitles.join(", ")}; titles=${JSON.stringify(titles(latestRows))}`,
  );
}

const wasm = await loadWasmModule();
const schema = app.wasmSchema;
const runtime = new NativeRuntimeAdapter(
  wasm.WasmDb,
  schema,
  deterministicRuntimeBytes(`${appId}:e2e-verifier:node`),
  deterministicRuntimeBytes(`${appId}:e2e-verifier:author`),
  1,
  false,
);
const client = JazzClient.connectWithRuntime(runtime, {
  appId,
  schema,
  serverUrl,
  userBranch: "e2e-verifier",
  tier: "local",
});

try {
  client.connectTransport(serverUrl, { admin_secret: adminSecret });
  const subscription = client.subscribe(JSON.stringify({ table: "todos" }), () => {}, {
    tier: "local",
  });

  try {
    const inserted = client.insert("todos", {
      title: { type: "Text", value: insertedTitle },
      done: { type: "Boolean", value: false },
      description: { type: "Null" },
      owner_id: { type: "Text", value: "e2e-verifier" },
      parentId: { type: "Null" },
    });
    await inserted.wait({ tier: "edge" });

    const syncedRows = await waitForTitles(client, [expectedTitle, insertedTitle]);
    console.log(
      JSON.stringify({
        observedOfflineTitle: expectedTitle,
        insertedRemoteTitle: insertedTitle,
        rowCount: syncedRows.length,
      }),
    );
  } finally {
    client.unsubscribe(subscription);
  }
} finally {
  await client.shutdown();
}
