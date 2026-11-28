import { createRequire } from "node:module";
import { describe, expect, it, vi } from "vitest";
import { testApp, type TestAppFile } from "../testing/test-app.js";
import { EncryptionError } from "../encryption/index.js";
import type { BackendNative } from "./backend-native.js";

// A Worker's engine exposes no key-holding client runtime.
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
describe("an app that declares encryption on the Cloudflare Worker engine", () => {
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
