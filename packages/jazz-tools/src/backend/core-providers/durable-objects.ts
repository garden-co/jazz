import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";
import { accountAppId } from "../../accounts/local-first.js";
import type { JazzCoreProvider } from "./core-provider.js";
import { openRemoteCore } from "./remote.js";

export interface DurableObjectsOptions {
  /** Stable Worker name. Changing this creates a different authority. */
  name: string;
  credentials?: { accountId: string; apiToken: string };
  /** Stable random core credential, independent of the Cloudflare API token.
   * Defaults to JAZZ_CORE_SECRET. Keep it across deployments. */
  secret?: string;
}
interface Manifest {
  format: string;
  version: string;
  workerSha256: string;
  wasmSha256: string;
  digest: string;
}
export interface DurableObjectArtifact {
  manifest: Manifest;
  worker: Uint8Array;
  wasm: Uint8Array;
}
const hash = (value: string | Uint8Array) => createHash("sha256").update(value).digest("hex");
export function verifyDurableObjectArtifact(artifact: DurableObjectArtifact): void {
  const { manifest: m, worker, wasm } = artifact;
  if (
    m.format !== "jazz-do-worker-v1" ||
    hash(worker) !== m.workerSha256 ||
    hash(wasm) !== m.wasmSha256 ||
    hash(`${m.format}\0${m.version}\0${m.workerSha256}\0${m.wasmSha256}`) !== m.digest
  )
    throw new Error("Invalid packaged Jazz Durable Object host");
}
async function loadArtifact(): Promise<DurableObjectArtifact> {
  const base = new URL("../../../durable-object-worker/", import.meta.url);
  const [manifest, worker, wasm] = await Promise.all([
    readFile(new URL("manifest.json", base), "utf8"),
    readFile(new URL("worker.mjs", base)),
    readFile(new URL("jazz.wasm", base)),
  ]);
  const artifact = { manifest: JSON.parse(manifest) as Manifest, worker, wasm };
  verifyDurableObjectArtifact(artifact);
  const pkg = JSON.parse(await readFile(new URL("../../../package.json", import.meta.url), "utf8"));
  if (pkg.version !== artifact.manifest.version)
    throw new Error("Jazz DO host/package version mismatch");
  return artifact;
}
interface Binding {
  name: string;
  type: string;
  text?: string;
  json?: unknown;
  class_name?: string;
  namespace_id?: string;
}
interface Settings {
  bindings: Binding[];
}
interface Marker {
  format: string;
  version: string;
  artifact: string;
  appId: string;
  secretHash: string;
}
// Only published numeric Jazz releases are accepted; never guess ordering of arbitrary tags.
function compareVersions(a: string, b: string): number {
  const parse = (v: string) => {
    const m = /^(\d+)\.(\d+)\.(\d+)(?:-alpha\.(\d+))?$/.exec(v);
    if (!m) throw new Error(`Unsupported Jazz host version: ${v}`);
    return [Number(m[1]), Number(m[2]), Number(m[3]), m[4] === undefined ? Infinity : Number(m[4])];
  };
  const x = parse(a),
    y = parse(b);
  for (let i = 0; i < x.length; i++) if (x[i] !== y[i]) return x[i] > y[i] ? 1 : -1;
  return 0;
}
interface Dependencies {
  fetch?: typeof globalThis.fetch;
  loadArtifact?: () => Promise<DurableObjectArtifact>;
  openRemote?: typeof openRemoteCore;
  sleep?: (ms: number) => Promise<void>;
}
/** Internal injection boundary: tests exercise the real HTTP provisioning protocol. */
export async function openDurableObjectsCore(
  options: DurableObjectsOptions,
  deps: Dependencies = {},
) {
  const account = options.credentials?.accountId ?? process.env.CLOUDFLARE_ACCOUNT_ID;
  const token = options.credentials?.apiToken ?? process.env.CLOUDFLARE_API_TOKEN;
  const secret = options.secret ?? process.env.JAZZ_CORE_SECRET;
  if (!account || !/^[a-f0-9]{32}$/.test(account) || !token)
    throw new Error("durableObjects requires Cloudflare account ID and API token");
  if (!/^[a-z][a-z0-9-]{0,61}[a-z0-9]$/.test(options.name))
    throw new Error("durableObjects requires a stable lowercase Worker name (2–63 characters)");
  if (!secret || secret.length < 32)
    throw new Error(
      "durableObjects requires a stable random core secret of at least 32 characters",
    );
  const artifact = await (deps.loadArtifact ?? loadArtifact)();
  verifyDurableObjectArtifact(artifact);
  const { manifest } = artifact;
  compareVersions(manifest.version, manifest.version);
  const appId = accountAppId(`jazz-durable-objects-v1:${account}:${options.name}`);
  const marker: Marker = {
    format: "jazz-do-host-v1",
    version: manifest.version,
    artifact: manifest.digest,
    appId,
    secretHash: hash(secret),
  };
  const fetcher = deps.fetch ?? globalThis.fetch;
  const base = `https://api.cloudflare.com/client/v4/accounts/${account}/workers`;
  const script = `/scripts/${options.name}`;
  async function api(path: string, init: RequestInit = {}, missing = false): Promise<any> {
    const response = await fetcher(base + path, {
      ...init,
      redirect: "error",
      signal: AbortSignal.timeout(30_000),
      headers: { ...init.headers, Authorization: `Bearer ${token}` },
    });
    if (missing && response.status === 404) return undefined;
    if (!response.ok)
      throw new Error(`Cloudflare ${init.method ?? "GET"} ${path}: HTTP ${response.status}`);
    const body = (await response.json()) as { success: boolean; result: unknown };
    if (!body.success) throw new Error(`Cloudflare rejected ${init.method ?? "GET"} ${path}`);
    return body.result;
  }
  const subdomain = (await api("/subdomain")).subdomain;
  if (typeof subdomain !== "string" || !/^[a-z0-9][a-z0-9-]*$/.test(subdomain))
    throw new Error("Configure an account workers.dev subdomain before opening Jazz");
  function inspect(settings: Settings | undefined): Marker | undefined {
    if (!settings) return undefined;
    const get = (name: string) => settings.bindings.find((b) => b.name === name);
    const raw = get("JAZZ_HOST")?.json;
    const prior = (typeof raw === "string" ? JSON.parse(raw) : raw) as Marker | undefined;
    if (
      !prior ||
      prior.format !== marker.format ||
      prior.appId !== appId ||
      get("JAZZ_APP_ID")?.text !== appId ||
      get("JAZZ")?.class_name !== "JazzObject" ||
      !get("JAZZ")?.namespace_id
    )
      throw new Error("Refusing to overwrite an unrecognized Jazz Worker or namespace");
    if (prior.secretHash !== marker.secretHash)
      throw new Error("Jazz core secret differs from the deployed authority");
    const order = compareVersions(prior.version, marker.version);
    if (order > 0) throw new Error("Refusing to downgrade a newer Jazz Worker");
    if (order === 0 && prior.artifact !== marker.artifact)
      throw new Error(
        "Same Jazz version has different host bytes; publish a new version before upgrading",
      );
    return prior;
  }
  let ready = false;
  for (let attempt = 0; attempt < 3; attempt++) {
    const settings = (await api(`${script}/settings`, {}, true)) as Settings | undefined;
    const prior = inspect(settings);
    if (prior?.artifact === marker.artifact) {
      ready = true;
      break;
    }
    const bindings: Binding[] = [
      {
        name: "JAZZ",
        type: "durable_object_namespace",
        class_name: "JazzObject",
        ...(prior
          ? { namespace_id: settings!.bindings.find((b) => b.name === "JAZZ")!.namespace_id }
          : {}),
      },
      { name: "JAZZ_APP_ID", type: "plain_text", text: appId },
      { name: "JAZZ_HOST", type: "json", json: marker },
      ...(!prior ? [{ name: "JAZZ_ADMIN_SECRET", type: "secret_text", text: secret }] : []),
    ];
    const metadata = {
      main_module: "worker.mjs",
      compatibility_date: "2026-07-01",
      bindings,
      ...(prior ? { keep_bindings: ["secret_text"] } : {}),
      migrations: {
        old_tag: prior ? `jazz-do-v1-${prior.artifact}` : "",
        new_tag: `jazz-do-v1-${marker.artifact}`,
        steps: prior ? [] : [{ new_sqlite_classes: ["JazzObject"] }],
      },
    };
    const form = new FormData();
    form.set("metadata", new Blob([JSON.stringify(metadata)], { type: "application/json" }));
    form.set(
      "worker.mjs",
      new Blob([new Uint8Array(artifact.worker)], { type: "application/javascript+module" }),
      "worker.mjs",
    );
    form.set(
      "jazz.wasm",
      new Blob([new Uint8Array(artifact.wasm)], { type: "application/wasm" }),
      "jazz.wasm",
    );
    try {
      await api(script, {
        method: "PUT",
        body: form,
        ...(!prior ? { headers: { "If-None-Match": "*" } } : {}),
      });
    } catch (error) {
      // A lost response or another opener may have installed this exact release.
      const current = inspect(await api(`${script}/settings`, {}, true));
      if (current?.artifact === marker.artifact) {
        ready = true;
        break;
      }
      if (current?.artifact === prior?.artifact) throw error;
      continue;
    }
  }
  if (!ready) {
    ready = inspect(await api(`${script}/settings`))?.artifact === marker.artifact;
    if (!ready) throw new Error("Jazz Worker changed concurrently; retry opening the provider");
  }
  const enabled = await api(`${script}/subdomain`);
  if (!enabled.enabled)
    await api(`${script}/subdomain`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ enabled: true, previews_enabled: false }),
    });
  const url = `https://${options.name}.${subdomain}.workers.dev`;
  const readinessStarted = Date.now();
  let readinessDeadline = readinessStarted + 120_000;
  let waitingForVersion = false;
  for (let attempt = 0; ; attempt++) {
    let response: Response | undefined;
    try {
      response = await fetcher(`${url}/apps/${appId}/admin/host`, {
        redirect: "error",
        signal: AbortSignal.timeout(Math.max(1, Math.min(10_000, readinessDeadline - Date.now()))),
        headers: { "X-Jazz-Backend-Credential": secret, "X-Jazz-Backend-Access": "admin" },
      });
    } catch {
      // New workers.dev hostnames can fail DNS/TLS before returning HTTP.
      // Retry only this read, never blindly repeat a deployment mutation.
    }
    if (response?.ok) {
      const live = (await response.json()) as Marker;
      if (live.appId === appId) {
        if (live.artifact === marker.artifact) break;
        // Active DOs defer code updates for up to five minutes by default.
        // Allow that window plus propagation, without extending cold-host or
        // credential failures or forcing existing sessions to reset early.
        waitingForVersion = true;
        readinessDeadline = readinessStarted + 420_000;
      }
    } else if (response?.status === 401)
      throw new Error("Deployed Jazz host refused its core credential");
    if (response && !response.bodyUsed) await response.body?.cancel();
    if (attempt === (waitingForVersion ? 55 : 15) || Date.now() >= readinessDeadline)
      throw new Error("Deployed Jazz host did not become ready at the expected version");
    await (deps.sleep ?? ((ms) => new Promise((resolve) => setTimeout(resolve, ms))))(
      Math.min(1000 * 2 ** attempt, 8000, readinessDeadline - Date.now()),
    );
  }
  return (deps.openRemote ?? openRemoteCore)(appId, {
    url,
    backendCredential: () => secret,
    secrets: { adminSecret: secret, backendSecret: secret },
  });
}
/** Provision a persistent Cloudflare authority and connect through the remote transport. */
export function durableObjects(options: DurableObjectsOptions): JazzCoreProvider {
  return {
    reuse: {
      key: `durable-objects:${options.credentials?.accountId ?? process.env.CLOUDFLARE_ACCOUNT_ID}:${options.name}`,
      settings: options,
    },
    open() {
      return openDurableObjectsCore((this.reuse?.settings ?? options) as DurableObjectsOptions);
    },
  };
}
