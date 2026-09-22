import { schema as s } from "published-tools/dist/schema-namespace.js";
import { createAccountManager } from "published-tools/dist/accounts/create-account-manager.js";
import { createDb } from "published-tools/dist/runtime/default-create-db.js";
window.run = async () => {
  const app = s.defineApp({ notes: s.table({ body: s.string() }) });
  const appId = "00000000-0000-4000-8000-000000000054";
  const manager = await createAccountManager({ appId, serverUrl: "http://127.0.0.1:1" });
  const account = manager.restoreLocalFirst(
    "jazz-auth-v1:AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8",
  );
  const config = {
    appId,
    account,
    runtimeSources: {
      wasmVersion: "published-alpha54",
      wasmUrl: "/jazz-wasm/package/pkg/jazz_wasm_bg.wasm",
      brokerWorkerUrl: "/jazz-tools/package/dist/worker/jazz-broker-worker.js",
    },
    driver: { type: "persistent", dbName: "published-alpha54-browser" },
  };
  let db = await createDb(config);
  const write = db.insert(app.notes, { body: "published alpha.54 original" });
  await write.wait({ tier: "local" });
  await db
    .update(app.notes, (await db.all(app.notes, { tier: "local" }))[0].id, {
      body: "published alpha.54 current",
    })
    .wait({ tier: "local" });
  const rows = await db.all(app.notes, { tier: "local" });
  await db.shutdown();
  db = await createDb(config);
  const reopened = await db.all(app.notes, { tier: "local" });
  if (reopened.length !== 1 || reopened[0].body !== "published alpha.54 current")
    throw new Error("published browser reopen lost current row");
  await db.shutdown();
  const names = (await indexedDB.databases())
    .map((x) => x.name)
    .filter((x) => x?.startsWith("published-alpha54-browser::"));
  if (names.length !== 1) throw new Error("expected exactly one physical root");
  const records = await rawRecords(names[0]);
  return { rows, records };
};

async function requestResult(request) {
  return await new Promise((resolve, reject) => {
    request.onsuccess = () => resolve(request.result);
    request.onerror = () => reject(request.error);
  });
}
function serialize(value) {
  if (value instanceof ArrayBuffer) return Array.from(new Uint8Array(value));
  if (ArrayBuffer.isView(value))
    return Array.from(new Uint8Array(value.buffer, value.byteOffset, value.byteLength));
  if (Array.isArray(value)) return value.map(serialize);
  if (value && typeof value === "object")
    return Object.fromEntries(Object.entries(value).map(([key, value]) => [key, serialize(value)]));
  return value;
}
async function rawRecords(name) {
  const database = await requestResult(indexedDB.open(name));
  const names = ["pages", "metadata", "storage-manifest"];
  const tx = database.transaction(names, "readonly");
  const done = new Promise((resolve, reject) => {
    tx.oncomplete = resolve;
    tx.onerror = () => reject(tx.error);
    tx.onabort = () => reject(tx.error);
  });
  const records = Object.fromEntries(
    await Promise.all(
      names.map(async (name) => {
        const store = tx.objectStore(name);
        const [keys, values] = await Promise.all([
          requestResult(store.getAllKeys()),
          requestResult(store.getAll()),
        ]);
        return [name, JSON.stringify(keys.map((key, i) => [key, serialize(values[i])]))];
      }),
    ),
  );
  await done;
  database.close();
  return records;
}
