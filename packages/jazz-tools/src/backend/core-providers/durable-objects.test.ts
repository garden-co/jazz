import { createHash } from "node:crypto";
import { expect, it, vi } from "vitest";
import { openDurableObjectsCore, verifyDurableObjectArtifact } from "./durable-objects.js";
const hash = (v: string | Uint8Array) => createHash("sha256").update(v).digest("hex");
function artifact(version = "2.0.0-alpha.60") {
  const worker = new TextEncoder().encode("export default {}"),
    wasm = new Uint8Array([0, 97, 115, 109]);
  const manifest = {
    format: "jazz-do-worker-v1",
    version,
    workerSha256: hash(worker),
    wasmSha256: hash(wasm),
    digest: "",
  };
  manifest.digest = hash(
    `${manifest.format}\0${version}\0${manifest.workerSha256}\0${manifest.wasmSha256}`,
  );
  return { manifest, worker, wasm };
}
const options = {
  name: "test-jazz",
  credentials: { accountId: "a".repeat(32), apiToken: "api-token" },
  secret: "core-secret-".repeat(4),
};
function cloud() {
  let settings: any,
    tag = "",
    enabled = false,
    lost = false;
  let hostFailures = 0,
    hostStatus = 200,
    hostCalls = 0;
  let staleReplies = 0;
  const uploads: any[] = [];
  const fetch = vi.fn(async (input: any, init: RequestInit = {}) => {
    const url = String(input),
      headers = new Headers(init.headers);
    const result = (value: unknown) => Response.json({ success: true, result: value });
    if (url.includes("workers.dev")) {
      hostCalls++;
      if (hostFailures-- > 0) throw new TypeError("fetch failed");
      if (hostStatus !== 200) return new Response(null, { status: hostStatus });
      expect(headers.get("authorization")).toBeNull();
      expect(headers.get("x-jazz-backend-credential")).toBe(options.secret);
      const marker = settings.bindings.find((b: any) => b.name === "JAZZ_HOST").json;
      return Response.json(
        staleReplies-- > 0 ? { ...marker, artifact: "previous-version" } : marker,
      );
    }
    expect(headers.get("authorization")).toBe("Bearer api-token");
    if (url.endsWith("/settings"))
      return settings ? result(settings) : new Response(null, { status: 404 });
    if (url.endsWith("/workers/subdomain")) return result({ subdomain: "example" });
    if (url.endsWith("/subdomain")) {
      if (init.method === "POST") enabled = true;
      return result({ enabled });
    }
    if (init.method === "PUT") {
      const form = init.body as FormData;
      const metadata = JSON.parse(await (form.get("metadata") as Blob).text());
      if (metadata.migrations.old_tag !== tag) return new Response(null, { status: 409 });
      expect((form.get("jazz.wasm") as Blob).type).toBe("application/wasm");
      const oldSecret = settings?.bindings.find((b: any) => b.name === "JAZZ_ADMIN_SECRET");
      settings = {
        bindings: metadata.bindings.map((b: any) =>
          b.name === "JAZZ" ? { ...b, namespace_id: b.namespace_id ?? "persisted-namespace" } : b,
        ),
      };
      if (oldSecret && metadata.keep_bindings?.includes("secret_text"))
        settings.bindings.push(oldSecret);
      tag = metadata.migrations.new_tag;
      uploads.push(metadata);
      if (lost) {
        lost = false;
        let hostFailures = 0,
          hostStatus = 200,
          hostCalls = 0;
        throw new Error("lost response");
      }
      return result({});
    }
    throw new Error(`Unexpected request ${url}`);
  }) as typeof globalThis.fetch;
  const openRemote = vi.fn(() => ({ close: vi.fn(async () => {}) })) as any;
  const open = (version = "2.0.0-alpha.60", opts = options) =>
    openDurableObjectsCore(opts, {
      fetch,
      loadArtifact: async () => artifact(version),
      openRemote,
      sleep: async () => {},
    });
  return {
    open,
    uploads,
    openRemote,
    hostFailures: (count: number) => {
      hostFailures = count;
    },
    hostStatus: (status: number) => {
      hostStatus = status;
    },
    hostCalls: () => hostCalls,
    staleReplies: (count: number) => {
      staleReplies = count;
    },
    setSettings: (s: any) => {
      settings = s;
    },
    loseReply: () => {
      lost = true;
    },
  };
}
it("creates once and reuses the authority after closing", async () => {
  const c = cloud();
  const core = await c.open();
  await core.close();
  await c.open();
  expect(c.uploads).toHaveLength(1);
  expect(c.openRemote.mock.calls[0][0]).toBe("18e4ec71-891d-561f-9f7a-e14ff2e6fc79");
  expect(c.uploads[0].migrations.steps).toEqual([{ new_sqlite_classes: ["JazzObject"] }]);
  expect(c.openRemote.mock.calls[0][1].url).toBe("https://test-jazz.example.workers.dev");
});
it("upgrades preserving namespace, identity and credential; refuses downgrade", async () => {
  const c = cloud();
  await c.open();
  await c.open("2.0.0-alpha.61");
  const upgrade = c.uploads[1];
  expect(upgrade.migrations.steps).toEqual([]);
  expect(upgrade.migrations.old_tag).toBe(c.uploads[0].migrations.new_tag);
  expect(upgrade.bindings.find((b: any) => b.name === "JAZZ").namespace_id).toBe(
    "persisted-namespace",
  );
  expect(upgrade.keep_bindings).toEqual(["secret_text"]);
  expect(upgrade.bindings.some((b: any) => b.name === "JAZZ_ADMIN_SECRET")).toBe(false);
  expect(c.openRemote.mock.calls[0][0]).toBe(c.openRemote.mock.calls[1][0]);
  await expect(c.open()).rejects.toThrow("downgrade");
  expect(c.uploads).toHaveLength(2);
});
it("recovers a lost upload response", async () => {
  const c = cloud();
  c.loseReply();
  await c.open();
  expect(c.uploads).toHaveLength(1);
});
it("concurrent openers converge", async () => {
  const c = cloud();
  await Promise.all([c.open(), c.open()]);
  expect(c.uploads).toHaveLength(1);
});
it("refuses unrelated Workers and changed credentials", async () => {
  const c = cloud();
  c.setSettings({ bindings: [] });
  await expect(c.open()).rejects.toThrow("unrecognized");
  expect(c.uploads).toHaveLength(0);
  c.setSettings(undefined);
  await c.open();
  await expect(c.open(undefined, { ...options, secret: "different".repeat(8) })).rejects.toThrow(
    "secret differs",
  );
  expect(c.uploads).toHaveLength(1);
});
it("rejects corrupted artifact bytes", () => {
  const a = artifact();
  a.wasm[0] = 99;
  expect(() => verifyDurableObjectArtifact(a)).toThrow("Invalid packaged");
});

it("pins the v1 artifact digest encoding", () => {
  expect(artifact().manifest.digest).toBe(
    "40035500b1e85343942f8d52a0fa583488122091bd1cb048e52b2320f91f0a0d",
  );
});

it("retries new-host network failures without repeating uploads", async () => {
  const c = cloud();
  c.hostFailures(2);
  await c.open();
  expect(c.hostCalls()).toBe(3);
  expect(c.uploads).toHaveLength(1);
});
it("bounds readiness retries and refuses invalid credentials immediately", async () => {
  const c = cloud();
  c.hostFailures(100);
  await expect(c.open()).rejects.toThrow("did not become ready");
  expect(c.hostCalls()).toBe(16);
  expect(c.openRemote).not.toHaveBeenCalled();
  const denied = cloud();
  denied.hostStatus(401);
  await expect(denied.open()).rejects.toThrow("refused its core credential");
  expect(denied.hostCalls()).toBe(1);
});

it("waits for an active object to adopt upgraded code beyond cold-host retries", async () => {
  const c = cloud();
  await c.open();
  c.staleReplies(40);
  await c.open("2.0.0-alpha.61");
  expect(c.hostCalls()).toBe(42);
  expect(c.uploads).toHaveLength(2);
});

it("also bounds readiness retries when an active object stays on old code", async () => {
  const c = cloud();
  c.staleReplies(100);
  await expect(c.open()).rejects.toThrow("did not become ready");
  expect(c.hostCalls()).toBe(56);
  expect(c.uploads).toHaveLength(1);
});

it("does not follow provisioning redirects with account credentials", async () => {
  const fetch = vi.fn(
    async (_input: RequestInfo | URL, _init?: RequestInit) =>
      new Response(null, {
        status: 302,
        headers: { location: "https://unrelated.example/" },
      }),
  );
  await expect(
    openDurableObjectsCore(options, {
      loadArtifact: async () => artifact(),
      fetch,
    }),
  ).rejects.toThrow("HTTP 302");
  expect(fetch).toHaveBeenCalledTimes(1);
  expect(fetch.mock.calls[0]?.[1]).toMatchObject({ redirect: "manual" });
});
