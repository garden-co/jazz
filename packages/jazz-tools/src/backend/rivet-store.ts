import { createHash } from "node:crypto";
import { openAsBlob } from "node:fs";
import { setTimeout as delay } from "node:timers/promises";

/** Jazz actor using RivetKit’s public actor-local SQLite API. */
export interface RivetStoreOptions {
  /** Stable database key. Use a different key for each independent database. */
  name: string;
  endpoint: string;
  namespace?: string;
  token: () => string | Promise<string>;
  /** Server-only secret, shared with the native host. At least 32 characters. */
  coreSecret: string;
  /** Existing pool name, or managed pool prefix (suffixed with its configuration digest). */
  pool: string;
  runtime:
    | { kind: "existing" }
    | {
        kind: "managed";
        cloudEndpoint: string;
        cloudToken: () => string | Promise<string>;
        organization?: string;
        cluster: string;
        region: string;
        /** A prebuilt `docker save` archive; see crates/jazz-rivet/build-image.sh. */
        image: { path: string; tag: string; sha256: string };
      };
  /** Bounds initial provisioning and readiness; default 15 minutes. */
  startupTimeoutMs?: number;
}

export function rivet(options: RivetStoreOptions): {
  readonly kind: "rivet";
  readonly options: RivetStoreOptions;
} {
  if (!/^[a-zA-Z0-9_-]+$/.test(options.name) || !/^[a-zA-Z0-9_-]+$/.test(options.pool)) {
    throw new Error(
      "rivet(): name and pool must contain only letters, digits, underscores and hyphens",
    );
  }
  if (options.coreSecret.length < 32)
    throw new Error("rivet(): coreSecret needs at least 32 characters");
  endpoint(options.endpoint);
  if (options.runtime.kind === "managed") {
    endpoint(options.runtime.cloudEndpoint);
    if (!/^[a-f0-9]{64}$/.test(options.runtime.image.sha256))
      throw new Error("rivet(): image.sha256 must be a SHA-256 hex digest");
  }
  return { kind: "rivet", options };
}

function endpoint(value: string): string {
  const url = new URL(value);
  if (
    url.username ||
    url.password ||
    url.search ||
    url.hash ||
    !(
      url.protocol === "https:" ||
      (url.protocol === "http:" && ["localhost", "127.0.0.1", "[::1]"].includes(url.hostname))
    )
  ) {
    throw new Error("rivet(): endpoints require HTTPS (HTTP is allowed on loopback)");
  }
  return value.replace(/\/$/, "");
}

type Pool = {
  env: Record<string, { string?: string; secret?: { name: string } }>;
  status: {
    state: string;
    action_needed: boolean;
    replicas?: { desired: number; ready: number; updated: number };
  };
};

/** Internal: provision once, then use ordinary Jazz HTTP and sync sessions. */
export async function openRivetStore(options: RivetStoreOptions) {
  rivet(options);
  const signal = AbortSignal.timeout(options.startupTimeoutMs ?? 15 * 60_000);
  const enc = encodeURIComponent;
  async function request(
    base: string,
    path: string,
    token: string,
    method = "GET",
    body?: unknown,
    missing = false,
    headers?: Record<string, string>,
  ): Promise<any> {
    const response = await fetch(`${endpoint(base)}${path}`, {
      method,
      signal,
      redirect: "error",
      headers: { authorization: `Bearer ${token}`, "content-type": "application/json", ...headers },
      ...(body === undefined ? {} : { body: JSON.stringify(body) }),
    });
    if (missing && response.status === 404) return undefined;
    // Don't echo a response or URL which could contain credentials.
    if (!response.ok)
      throw new Error(`rivet(): ${method} ${path.split("?")[0]} returned HTTP ${response.status}`);
    return response.status === 204 ? undefined : response.json();
  }
  const metadata = (await request(options.endpoint, "/metadata", await options.token())) as {
    version: string;
  };
  const major = /^v?(\d+)\./.exec(metadata.version)?.[1];
  if (!major || Number(major) < 3)
    throw new Error("rivet(): Rivet Engine 3.0 or newer is required");
  const runtime = options.runtime;
  let poolName = options.pool;
  if (runtime.kind === "managed") {
    const cluster = `/v2/clusters/${enc(runtime.cluster)}`;
    // Concurrent first starts with different settings must never overwrite each
    // other's pools. Same-settings starts can safely upsert equivalent builds.
    const configHash = createHash("sha256")
      .update(
        JSON.stringify([runtime.image.sha256, options.coreSecret, options.namespace ?? "default"]),
      )
      .digest("hex")
      .slice(0, 20);
    poolName = `${options.pool}-${configHash}`;
    const poolPath = `${cluster}/regions/${enc(runtime.region)}/runner-pools/${enc(poolName)}`;
    const api = async (path: string, method = "GET", body?: unknown, missing = false) =>
      request(
        runtime.cloudEndpoint,
        path,
        await runtime.cloudToken(),
        method,
        body,
        missing,
        runtime.organization ? { "x-rivet-organization": runtime.organization } : undefined,
      );
    // Resolve the region first: a missing cluster/region is not a missing pool.
    const regions = (await api(`${cluster}/regions`)) as { items: { region: string }[] };
    if (!regions.items.some((r) => r.region === runtime.region))
      throw new Error("rivet(): cluster does not contain the requested region");
    const secretHash = createHash("sha256").update(options.coreSecret).digest("hex");
    const secretName = `jazz-${options.pool}-${secretHash.slice(0, 20)}`;
    const expected = {
      JAZZ_HOST_ARTIFACT: { string: runtime.image.sha256 },
      JAZZ_CORE_SECRET: { secret: { name: secretName } },
      RIVET_NAMESPACE: { string: options.namespace ?? "default" },
    };
    function checkPool(pool: Pool) {
      for (const [name, value] of Object.entries(expected)) {
        const actual = pool.env[name];
        if (
          "string" in value
            ? actual?.string !== value.string
            : actual?.secret?.name !== value.secret.name
        ) {
          throw new Error(
            "rivet(): existing pool belongs to another host build, namespace or secret; choose a dedicated pool",
          );
        }
      }
    }
    let pool = (await api(poolPath, "GET", undefined, true)) as Pool | undefined;
    if (!pool) {
      // openAsBlob streams from disk and detects mutation while it is being read.
      const tar = await openAsBlob(runtime.image.path);
      const sha = createHash("sha256"),
        md5 = createHash("md5");
      for await (const chunk of tar.stream()) {
        signal.throwIfAborted();
        sha.update(chunk);
        md5.update(chunk);
      }
      if (sha.digest("hex") !== runtime.image.sha256)
        throw new Error("rivet(): image archive SHA-256 mismatch");
      const digest = md5.digest("base64");
      const build = (await api(`${cluster}/builds`, "POST", {
        image: runtime.image.tag,
        content_length: tar.size,
        content_md5: digest,
      })) as { build_id: string; upload_url: string };
      endpoint(build.upload_url.split("?")[0]);
      const uploaded = await fetch(build.upload_url, {
        method: "PUT",
        body: tar,
        signal,
        redirect: "error",
        headers: { "content-length": String(tar.size), "content-md5": digest },
      });
      if (!uploaded.ok) throw new Error(`rivet(): image upload returned HTTP ${uploaded.status}`);
      const buildPath = `${cluster}/builds/${enc(build.build_id)}`;
      await api(`${buildPath}/complete`, "POST");
      while (true) {
        const state = (await api(buildPath)) as { status: string };
        if (state.status === "ready") break;
        if (!["uploading", "completing", "preparing"].includes(state.status))
          throw new Error(`rivet(): image build ${state.status}`);
        await delay(2000, undefined, { signal });
      }
      // Recheck after the long upload; never replace an incompatible pool.
      pool = (await api(poolPath, "GET", undefined, true)) as Pool | undefined;
      if (pool) checkPool(pool);
      else {
        await api(`${cluster}/secrets/rollout`, "POST", {
          name: secretName,
          value: options.coreSecret,
        });
        pool = (await api(poolPath, "PUT", {
          build_id: build.build_id,
          cidr_allowlist: [],
          env: expected,
        })) as Pool;
      }
    }
    while (true) {
      checkPool(pool);
      if (pool.status.action_needed)
        throw new Error("rivet(): runner pool needs attention in the Rivet dashboard");
      const replicas = pool.status.replicas;
      if (
        pool.status.state === "ready" &&
        replicas &&
        replicas.desired > 0 &&
        replicas.ready >= replicas.desired &&
        replicas.updated >= replicas.desired
      )
        break;
      await delay(2000, undefined, { signal });
      pool = (await api(poolPath)) as Pool;
    }
  }
  const result = (await request(
    options.endpoint,
    `/actors?namespace=${enc(options.namespace ?? "default")}`,
    await options.token(),
    "PUT",
    {
      name: "jazz",
      key: options.name,
      pool: poolName,
    },
  )) as { actor: { actor_id: string; pool: string } };
  if (!result.actor.actor_id || result.actor.pool !== poolName)
    throw new Error("rivet(): database key resolves to a different actor pool");
  const gateway = `${endpoint(options.endpoint)}/gateway/${enc(result.actor.actor_id)}`;
  const headers = async () => ({ "x-rivet-token": await options.token() });
  const response = await fetch(`${gateway}/request/__jazz/host`, {
    signal,
    redirect: "error",
    headers: { ...(await headers()), "x-jazz-backend-credential": options.coreSecret },
  });
  if (!response.ok) throw new Error(`rivet(): actor startup returned HTTP ${response.status}`);
  const host = (await response.json()) as {
    appId: string;
    artifact: string;
  };
  if (
    !/^[a-f0-9-]{36}$/.test(host.appId) ||
    (runtime.kind === "managed" && host.artifact !== runtime.image.sha256)
  ) {
    throw new Error("rivet(): actor returned an invalid app ID or unexpected image artifact");
  }
  return {
    appId: host.appId,
    url: `${gateway}/request`,
    socketUrl: `${gateway.replace(/^http/, "ws")}/websocket/apps/${enc(host.appId)}/ws`,
    headers,
    backendCredential: () => options.coreSecret,
    secrets: { adminSecret: options.coreSecret, backendSecret: options.coreSecret },
  };
}
