import { afterEach, describe, expect, it, vi } from "vitest";
import { createHash } from "node:crypto";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { openRivetStore, rivet, type RivetStoreOptions } from "./rivet-store.js";

const appId = "1d9be155-3aa6-4988-9fd1-a497ee720ae5";
const base: RivetStoreOptions = {
  name: "notes",
  endpoint: "https://engine.example",
  namespace: "test",
  token: () => "engine-credential",
  coreSecret: "x".repeat(32),
  pool: "jazz-test",
  runtime: { kind: "existing" },
};
const host = {
  appId,
  artifact: "development",
};
const dirs: string[] = [];
afterEach(async () => {
  vi.unstubAllGlobals();
  vi.unstubAllEnvs();
  await Promise.all(dirs.splice(0).map((dir) => rm(dir, { recursive: true, force: true })));
});

function server(run: (url: URL, init: RequestInit) => unknown | Promise<unknown>) {
  const fetch = vi.fn(async (url: string, init: RequestInit = {}) => {
    const parsed = new URL(url);
    const result = parsed.pathname === "/metadata" ? { version: "3.0.7" } : await run(parsed, init);
    return result instanceof Response ? result : Response.json(result);
  });
  vi.stubGlobal("fetch", fetch);
  return fetch;
}

describe("experimental Rivet store", () => {
  it("defers deployment configuration until opening", async () => {
    vi.stubEnv("RIVET_ENDPOINT", undefined);
    const fetch = vi.fn();
    vi.stubGlobal("fetch", fetch);
    const store = rivet({ name: "notes", pool: "jazz-test" });
    expect(store.kind).toBe("rivet");
    await expect(openRivetStore(store.options)).rejects.toThrow("endpoint or RIVET_ENDPOINT");
    expect(fetch).not.toHaveBeenCalled();
  });

  it("uses Rivet's deployment endpoint and the shared core secret", async () => {
    vi.stubEnv("RIVET_ENDPOINT", "https://test:encoded%3Atoken@engine.example");
    vi.stubEnv("JAZZ_CORE_SECRET", "s".repeat(32));
    const fetch = server((url, init) => {
      expect(url.username).toBe("");
      expect(url.password).toBe("");
      if (url.pathname === "/actors") {
        expect(url.searchParams.get("namespace")).toBe("test");
        expect(new Headers(init.headers).get("authorization")).toBe("Bearer encoded:token");
        return { actor: { actor_id: "a", pool: "jazz-test" } };
      }
      expect(new Headers(init.headers).get("x-jazz-backend-credential")).toBe("s".repeat(32));
      return host;
    });
    const target = await openRivetStore({ name: "notes", pool: "jazz-test" });
    expect(target.appId).toBe(appId);
    expect(fetch).toHaveBeenCalledTimes(3);
  });

  it("refreshes a separate environment token and honors explicit overrides", async () => {
    vi.stubEnv("RIVET_ENDPOINT", "https://engine.example");
    vi.stubEnv("RIVET_NAMESPACE", "environment");
    vi.stubEnv("RIVET_TOKEN", "first");
    vi.stubEnv("JAZZ_CORE_SECRET", "s".repeat(32));
    server((url) => {
      if (url.pathname === "/actors") {
        expect(url.searchParams.get("namespace")).toBe("explicit");
        return { actor: { actor_id: "a", pool: "jazz-test" } };
      }
      return host;
    });
    const target = await openRivetStore({
      name: "notes",
      pool: "jazz-test",
      namespace: "explicit",
    });
    vi.stubEnv("RIVET_TOKEN", "renewed");
    expect(await target.headers()).toEqual({ "x-rivet-token": "renewed" });
    const explicit = await openRivetStore({ ...base, namespace: "explicit" });
    expect(await explicit.headers()).toEqual({ "x-rivet-token": "engine-credential" });
    expect(explicit.backendCredential()).toBe(base.coreSecret);
  });

  it("refuses Rivet 2 before provisioning resources", async () => {
    const fetch = vi.fn(async () => Response.json({ version: "2.9.0" }));
    vi.stubGlobal("fetch", fetch);
    await expect(openRivetStore(base)).rejects.toThrow("Engine 3.0 or newer");
    expect(fetch).toHaveBeenCalledTimes(1);
  });

  it("creates the keyed actor and wakes it through the gateway with separate credentials", async () => {
    let token = "engine-credential";
    const fetch = server((url, init) => {
      if (url.pathname === "/actors") {
        expect(url.searchParams.get("namespace")).toBe("test");
        expect(init.method).toBe("PUT");
        expect(JSON.parse(init.body as string)).toEqual({
          name: "jazz",
          key: "notes",
          pool: "jazz-test",
        });
        expect(new Headers(init.headers).get("authorization")).toBe("Bearer engine-credential");
        return { actor: { actor_id: "actor-one", pool: "jazz-test" } };
      }
      expect(url.pathname).toBe("/gateway/actor-one/request/__jazz/host");
      expect(new Headers(init.headers).get("x-rivet-token")).toBe("engine-credential");
      expect(new Headers(init.headers).get("x-jazz-backend-credential")).toBe(base.coreSecret);
      return host;
    });
    const target = await openRivetStore({ ...base, token: () => token });
    expect(fetch).toHaveBeenCalledTimes(3);
    expect(target.url).toBe("https://engine.example/gateway/actor-one/request");
    expect(target.socketUrl).toBe(
      `wss://engine.example/gateway/actor-one/websocket/apps/${appId}/ws`,
    );
    token = "renewed-credential";
    expect(await target.headers()).toEqual({ "x-rivet-token": token });
  });

  it("refuses a database key that already belongs to another pool", async () => {
    const fetch = server(() => ({ actor: { actor_id: "actor-one", pool: "someone-else" } }));
    await expect(openRivetStore(base)).rejects.toThrow("different actor pool");
    expect(fetch).toHaveBeenCalledTimes(2);
  });

  it("refuses an invalid app ID before applying any Jazz schema", async () => {
    server((url) =>
      url.pathname === "/actors"
        ? { actor: { actor_id: "a", pool: base.pool } }
        : { ...host, appId: "invalid" },
    );
    await expect(openRivetStore(base)).rejects.toThrow("invalid app ID");
  });

  it("uploads a verified image, provisions once, then reuses the ready pool", async () => {
    const dir = await mkdtemp(join(tmpdir(), "jazz-rivet-test-"));
    dirs.push(dir);
    const bytes = Buffer.from("fixture docker archive");
    const path = join(dir, "image.tar");
    await writeFile(path, bytes);
    const sha256 = createHash("sha256").update(bytes).digest("hex");
    const options: RivetStoreOptions = {
      ...base,
      runtime: {
        kind: "managed",
        cloudEndpoint: "https://cloud.example",
        cloudToken: () => "cloud-credential",
        cluster: "cluster",
        region: "region",
        image: { path, tag: "jazz:test", sha256 },
      },
    };
    let pool: unknown,
      builds = 0,
      uploads = 0,
      creations = 0;
    server(async (url, init) => {
      const headers = new Headers(init.headers);
      if (url.hostname === "upload.example") {
        uploads++;
        expect(headers.has("authorization")).toBe(false);
        expect(headers.get("content-md5")).toBe(createHash("md5").update(bytes).digest("base64"));
        expect(Buffer.from(await (init.body as Blob).arrayBuffer())).toEqual(bytes);
        return {};
      }
      if (url.hostname === "engine.example")
        return url.pathname === "/actors"
          ? { actor: { actor_id: "a", pool: JSON.parse(init.body as string).pool } }
          : { ...host, artifact: sha256 };
      expect(headers.get("authorization")).toBe("Bearer cloud-credential");
      if (url.pathname.endsWith("/regions")) return { items: [{ region: "region" }] };
      if (url.pathname.includes("/runner-pools/jazz-test-")) {
        if (init.method === "PUT") {
          creations++;
          const body = JSON.parse(init.body as string);
          expect(JSON.stringify(body)).not.toContain(base.coreSecret);
          pool = {
            ...body,
            status: {
              state: "ready",
              action_needed: false,
              replicas: { desired: 1, ready: 1, updated: 1 },
            },
          };
        }
        return pool ?? new Response(null, { status: 404 });
      }
      if (url.pathname.endsWith("/builds")) {
        builds++;
        return {
          build_id: "build-one",
          upload_url: "https://upload.example/image?signature=secret",
        };
      }
      if (url.pathname.includes("/builds/build-one")) return { status: "ready" };
      if (url.pathname.endsWith("/secrets/rollout")) {
        expect(JSON.parse(init.body as string).value).toBe(base.coreSecret);
        return {};
      }
      throw new Error(`Unexpected test URL ${url.pathname}`);
    });
    await openRivetStore(options);
    // Reuse doesn't require the archive still to be available.
    await rm(path);
    await openRivetStore(options);
    expect([builds, uploads, creations]).toEqual([1, 1, 1]);
    (pool as { env: Record<string, unknown> }).env.JAZZ_HOST_ARTIFACT = { string: "other" };
    await expect(openRivetStore(options)).rejects.toThrow("another host build");
    expect(creations).toBe(1);
  });

  it("rejects unsafe endpoints and ambiguous actor keys before making requests", () => {
    expect(() => rivet({ ...base, name: "a/b" })).toThrow("name and pool");
    expect(() => rivet({ ...base, endpoint: "http://engine.example" })).toThrow("HTTPS");
  });
});
