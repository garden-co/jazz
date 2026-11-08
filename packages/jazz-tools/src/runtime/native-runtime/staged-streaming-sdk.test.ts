import { expect, it, onTestFinished } from "vitest";
import { schema as s } from "../../schema-namespace.js";
import { mergePermissionsIntoWasmSchema } from "../../schema-permissions.js";
import { JazzClient, createOpenTransactionId } from "../client.js";
import type { Session } from "../context.js";
import { allowAll } from "../testing/allow-all.js";
import { testAuthorBytes } from "../testing/account-fixtures.js";
import { loadNapiModule } from "../testing/napi-runtime-test-utils.js";
import { loadWasmModuleForTest } from "../testing/wasm-runtime-test-utils.js";
import { openConfig } from "./native-codec.js";
import { encodeSchema } from "./schema-codec.js";
import { NativeRuntimeAdapter } from "./native-runtime-adapter.js";
import { startLocalJazzServer } from "../../dev/dev-server.js";
import { deploy } from "../../dev/catalogue.js";

const app = s.defineApp({ files: s.table({ payload: s.bytes() }, {}) }, { migrations: "none" });
const session: Session = {
  issuer: "https://issuer.example",
  user_id: "explicit-reader",
  claims: { role: "writer" },
  authMode: "external",
};
const readOnly = s.definePermissions(app, ({ policy }) => {
  policy.files.allowRead.always();
  policy.files.allowInsert.never();
});

async function open(host: "NAPI" | "WASM", denyWrites = false) {
  const schema = mergePermissionsIntoWasmSchema(
    app.wasmSchema,
    denyWrites ? readOnly : allowAll(app),
  );
  if (denyWrites) expect(schema.files!.policies?.insert?.with_check).toEqual({ type: "False" });
  const ctor =
    host === "NAPI" ? (await loadNapiModule()).NapiDb : (await loadWasmModuleForTest()).WasmDb;
  const node = new Uint8Array(16).fill(61);
  const author = testAuthorBytes(`staged-sdk:${host}:owner`);
  const native = ctor.openMemory(encodeSchema(schema), openConfig(node, author, 1, false));
  const runtime = NativeRuntimeAdapter.fromDb(native as never, schema, node, author, 1, false);
  onTestFinished(() => runtime.close());
  const context = { appId: "staged-sdk", schema, cookieSession: session };
  const client = JazzClient.connectWithRuntime(runtime, context);
  return {
    schema,
    runtime,
    client,
    context,
    rows: () => runtime.query(JSON.stringify({ table: "files" }), undefined, "local-only"),
  };
}

async function* bytes() {
  yield new Uint8Array([4, 5, 6]);
}

for (const host of ["NAPI", "WASM"] as const) {
  it(`${host} SDK staged publication keeps the actual implicit opening actor`, async () => {
    const { client, rows } = await open(host);
    const staged = await client.stageStreamingMutation(
      "insert",
      "files",
      {},
      "payload",
      bytes(),
      undefined,
      undefined,
      undefined,
      undefined,
      "client-accepted-evidence",
    );
    const tx = client.openTransaction(
      "serializable",
      undefined,
      undefined,
      undefined,
      undefined,
      "client-accepted-evidence",
    );
    await client.prepareTransaction(tx, (io) => io.attachStreamingMutation(staged));
    await client.commitTransaction(tx).txId;
    expect(await rows()).toHaveLength(1);
    await expect(staged.attach(tx)).rejects.toThrow(/consumed/);
    expect(await staged.abort()).toBe(false);
  });

  it(`${host} SDK staged implicit authenticated evidence matches Session-owner opening`, async () => {
    const { client, rows } = await open(host);
    const staged = await client.stageStreamingMutation(
      "insert",
      "files",
      {},
      "payload",
      bytes(),
      undefined,
      undefined,
      undefined,
      undefined,
      "accepted-evidence",
    );
    const tx = client.openTransaction(
      "serializable",
      undefined,
      undefined,
      undefined,
      undefined,
      "accepted-evidence",
    );
    await client.prepareTransaction(tx, (io) => io.attachStreamingMutation(staged));
    await client.commitTransaction(tx).txId;
    expect(await rows()).toHaveLength(1);
    expect(await staged.abort()).toBe(false);
  });

  it(`${host} SDK staged explicit sessions remain explicit and own-client overrides refuse before input`, async () => {
    const { client, rows } = await open(host);
    let pulls = 0;
    const source = async function* () {
      pulls++;
      yield new Uint8Array([1]);
    };
    await expect(
      client.stageStreamingMutation(
        "insert",
        "files",
        {},
        "payload",
        source(),
        undefined,
        session,
        undefined,
        undefined,
        "client-accepted-evidence",
      ),
    ).rejects.toThrow(/cannot select another session/);
    expect(pulls).toBe(0);
    const staged = await client.stageStreamingMutation(
      "insert",
      "files",
      {},
      "payload",
      source(),
      undefined,
      session,
      undefined,
      undefined,
      "accepted-evidence",
    );
    const tx = client.openTransaction(
      "serializable",
      session,
      undefined,
      undefined,
      undefined,
      "accepted-evidence",
    );
    await client.prepareTransaction(tx, (io) => io.attachStreamingMutation(staged));
    await client.commitTransaction(tx).txId;
    expect(pulls).toBe(1);
    expect(await rows()).toHaveLength(1);
  });

  it(`${host} SDK staged actor mismatch consumes rather than rebinding the upload`, async () => {
    const { client, rows } = await open(host);
    const staged = await client.stageStreamingMutation(
      "insert",
      "files",
      {},
      "payload",
      bytes(),
      undefined,
      undefined,
      undefined,
      undefined,
      "client-accepted-evidence",
    );
    const tx = client.openTransaction(
      "serializable",
      session,
      undefined,
      undefined,
      undefined,
      "accepted-evidence",
    );
    await expect(
      client.prepareTransaction(tx, (io) => io.attachStreamingMutation(staged)),
    ).rejects.toThrow(/mix write identities/);
    expect(await staged.abort()).toBe(false);
    await client.rollbackTransaction(tx);
    expect(await rows()).toEqual([]);
  });

  it(`${host} SDK staged wrong-client attachment refuses and releases the capability`, async () => {
    const { client, runtime, context, rows } = await open(host);
    const staged = await client.stageStreamingMutation("insert", "files", {}, "payload", bytes());
    const other = JazzClient.connectWithRuntime(runtime, context);
    const tx = other.beginTransaction("serializable");
    await expect(
      other.prepareTransaction(tx, (io) => io.attachStreamingMutation(staged)),
    ).rejects.toThrow(/another client/);
    expect(await staged.abort()).toBe(false);
    await other.rollbackTransaction(tx);
    expect(await rows()).toEqual([]);
  });

  it(`${host} SDK staged attachment refuses the wrong schema without publishing`, async () => {
    const { runtime, schema, rows } = await open(host);
    const view = runtime.registerSchemaView({
      ...schema,
      files: {
        ...schema.files!,
        columns: [
          ...schema.files!.columns,
          { name: "note", column_type: { type: "Text" }, nullable: true },
        ],
      },
    });
    const staged = await view.stageStreamingMutation("insert", "files", {}, "payload", bytes());
    const tx = createOpenTransactionId();
    runtime.beginTransaction("serializable", tx);
    await expect(staged.attach(tx)).rejects.toThrow(/schema/);
    expect(await staged.abort()).toBe(false);
    await runtime.rollbackTransaction(tx);
    expect(await rows()).toEqual([]);
  });

  it(`${host} SDK staged matching schema view retains its own publication context`, async () => {
    const { runtime, schema, context, rows } = await open(host);
    const view = runtime.registerSchemaView({
      ...schema,
      files: {
        ...schema.files!,
        columns: [
          ...schema.files!.columns,
          { name: "note", column_type: { type: "Text" }, nullable: true },
        ],
      },
    });
    const client = JazzClient.connectWithRuntime(view, context);
    const staged = await client.stageStreamingMutation("insert", "files", {}, "payload", bytes());
    const tx = client.beginTransaction("serializable");
    await client.prepareTransaction(tx, (io) => io.attachStreamingMutation(staged));
    await client.commitTransaction(tx).txId;
    expect(await rows()).toHaveLength(1);
    expect(await staged.abort()).toBe(false);
  });

  it(`${host} SDK staged close and invalid-transaction attempts are terminal`, async () => {
    const { runtime } = await open(host);
    const invalid = await runtime.stageStreamingMutation("insert", "files", {}, "payload", bytes());
    await expect(invalid.attach(createOpenTransactionId())).rejects.toThrow(/open serializable/);
    expect(await invalid.abort()).toBe(false);
    const staged = await runtime.stageStreamingMutation("insert", "files", {}, "payload", bytes());
    const tx = createOpenTransactionId();
    runtime.beginTransaction("serializable", tx);
    await runtime.close();
    await expect(staged.attach(tx)).rejects.toThrow(/closed/);
    expect(await staged.abort()).toBe(false);
    await expect(staged.attach(tx)).rejects.toThrow(/consumed/);
  });

  it(`${host} SDK staged normal policy refusal prevents Global acceptance`, async () => {
    const server = await startLocalJazzServer({
      inMemory: true,
      schema: app.wasmSchema,
      permissions: readOnly,
    });
    onTestFinished(() => server.stop());
    const { bundle } = await deploy({
      appId: server.appId,
      serverUrl: server.url,
      adminSecret: server.adminSecret,
      schema: app,
      permissions: readOnly,
    });
    const { client, runtime, rows } = await open(host, true);
    // The normal host-test transport pins the exact published permission bundle.
    // The staged operation still carries its explicit user, never SYSTEM.
    client.connectTransport(server.url, { admin_secret: server.adminSecret, bundle });
    await runtime.query(JSON.stringify({ table: "files" }), undefined, "remote");
    const staged = await client.stageStreamingMutation(
      "insert",
      "files",
      {},
      "payload",
      bytes(),
      undefined,
      session,
      undefined,
      undefined,
      "accepted-evidence",
    );
    const tx = client.openTransaction(
      "serializable",
      session,
      undefined,
      undefined,
      undefined,
      "accepted-evidence",
    );
    // A replica may durably stage a provisional write. Only the real authority
    // decides whether its policy allows Global acceptance.
    await staged.attach(tx);
    expect(await rows()).toEqual([]);
    const receipt = runtime.commitTransaction(tx);
    await runtime.waitForTransaction(receipt, "local");
    await expect(runtime.waitForTransaction(receipt, "global")).rejects.toThrow(
      /forbidden|permission|denied|policy/i,
    );
    expect(await staged.abort()).toBe(false);
    expect(await rows()).toEqual([]);
  });
}
