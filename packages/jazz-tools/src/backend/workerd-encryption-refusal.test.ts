import { createRequire } from "node:module";
import { afterEach, describe, expect, it, vi } from "vitest";
import { testApp, type TestAppFile } from "../testing/test-app.js";
import { EncryptionError } from "../encryption/index.js";
import type { BackendNative } from "./backend-native.js";

// A white-box test: the backend sees the Worker engine (host `workerd`, no
// encryption runtime) through `backendNative()`, while the database calls
// stay on jazz-napi. The real Worker engine is jazz-wasm's backend build,
// which this suite does not build; it has no keyless openers yet, so an app
// that declares encryption must be refused at open with a clear error,
// never opened without its keyless hooks.
vi.mock("./backend-native.js", async (importOriginal) => {
  const actual = await importOriginal<typeof import("./backend-native.js")>();
  const { importNodePeer } = await import("./node-peers.js");
  let engine: BackendNative | undefined;
  const workerd = (): BackendNative =>
    (engine ??= {
      ...actual.napiBackendNative(createRequire(import.meta.url)("jazz-napi"), importNodePeer),
      host: "workerd",
      EncryptionRuntime: undefined,
    });
  return { ...actual, backendNative: workerd };
});

const { createJazz, memory } = await import("./create-jazz.js");
const { buildNodeEncryption } = await import("../encryption/node-host.js");
const { memoryPrivateStore } = await import("../encryption/index.js");

// Projects are spaces; a task's title is encrypted in its project's space.
const encrypted = {
  format: 1,
  tables: {
    projects: { columns: { name: "string", ownerId: "uuid" } },
    tasks: {
      columns: { projectId: "uuid", title: "bytes" },
      relations: { project: { rel: "projects", column: "projectId" } },
      encryption: { space: "projectId", scope: "projects", columns: { title: "string" } },
    },
  },
} as const satisfies TestAppFile;
const permissions = {
  $schema: "https://jazz.tools/schemas/jazz.permissions.v1.json",
  tables: {
    projects: {
      read: "exists __e2ee_members(scopeId = table_id('projects'), identifier = this.id, accountId = session.user)",
      insert: "$createdBy.account = session.user",
    },
    tasks: { read: "allowedTo.read(project)", insert: "true" },
  },
} as never;

const cleanups: Array<() => Promise<unknown>> = [];
afterEach(async () => {
  while (cleanups.length > 0) await cleanups.pop()!();
});

describe("an app that declares encryption on the Cloudflare Worker engine", () => {
  it("is refused at open by createJazz, naming the Worker", async () => {
    const jazz = createJazz({ app: testApp(encrypted), permissions, store: memory() });
    cleanups.push(() => jazz.close().catch(() => undefined));
    await expect(jazz.ready()).rejects.toThrow(
      "createJazz: opening an app that declares end-to-end encryption is not supported in a Cloudflare Worker yet",
    );
  });

  it("gets no key-holding backend client: building its encryption refuses with a typed error", async () => {
    const refused = buildNodeEncryption({
      app: testApp(encrypted),
      appId: "00000000-0000-0000-0000-000000000001",
      environment: "dev",
      accountId: "00000000-0000-0000-0000-000000000002",
      encryption: { store: memoryPrivateStore() },
    });
    await expect(refused).rejects.toBeInstanceOf(EncryptionError);
    await expect(refused).rejects.toMatchObject({
      code: "unsupported",
      message:
        "A backend client of an app that declares end-to-end encryption is not supported in a Cloudflare Worker yet",
    });
  });

  it("does not refuse a plain app: the check is the encryption declaration", async () => {
    const plain = { format: 1, tables: { notes: { columns: { body: "string" } } } };
    await expect(
      buildNodeEncryption({
        app: testApp(plain),
        appId: "00000000-0000-0000-0000-000000000001",
        environment: "dev",
        accountId: "00000000-0000-0000-0000-000000000002",
        encryption: undefined,
      }),
    ).resolves.toBeUndefined();
  });
});
