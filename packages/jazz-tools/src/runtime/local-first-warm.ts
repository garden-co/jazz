import type { StorageDriver } from "../drivers/types.js";
import type { RuntimeSourcesConfig } from "./context.js";
import { loadWasmModule } from "./wasm-loader.js";
import {
  rememberedBrowserWorkerRoot,
  warmBrowserWorker,
} from "./native-runtime/browser-shared-worker-connection.js";
import { browserWorkerRuntimeSources } from "./default-runtime-source.js";

/** @internal What `warmUp` needs to know before the session has an account. */
export interface LocalFirstWarmUpConfig {
  /** The session's account registry (`clientRegistryUrl`). */
  registry: string;
  runtimeSources?: RuntimeSourcesConfig;
  driver?: StorageDriver;
  /**
   * Whether to start the worker of the root this registry's last lease
   * opened. Off where the page must not reopen anything of the last user
   * before the backend answers (`clearOnSignOut`).
   */
  worker: boolean;
}

/**
 * @internal Start the local-first client's account-independent work before
 * the session resolves its account: the page's WASM fetch and compile, and
 * the broker worker of the root the last local-first lease opened (with its
 * own WASM prefetch). Nothing here admits, claims or opens a store; the
 * client the session opens afterwards does all of that for the account it
 * actually has, and reuses what this started when it names the same WASM and
 * the same worker.
 *
 * Resolves once the worker answered its probe (or there is none to start)
 * and the page's WASM request is out; the compiles continue in the
 * background. Never rejects.
 */
export async function warmUpLocalFirst(config: LocalFirstWarmUpConfig): Promise<void> {
  if (typeof window === "undefined" || typeof document === "undefined") return;
  const sources = config.runtimeSources;
  // In-memory WASM instantiates synchronously: nothing to overlap.
  if (sources?.wasmModule || sources?.wasmSource) return;
  let worker: Promise<unknown> = Promise.resolve();
  const dbName =
    config.worker &&
    (config.driver?.type ?? "persistent") === "persistent" &&
    !sources?.browserWorkerPort &&
    typeof SharedWorker !== "undefined"
      ? rememberedBrowserWorkerRoot(config.registry)
      : null;
  if (dbName) worker = warmBrowserWorker(browserWorkerRuntimeSources(config), dbName);
  // The same call the client's runtime source makes: it reuses this load.
  void loadWasmModule(sources).catch(() => undefined);
  // Let the loader import its glue and send the WASM request.
  const requested = import("jazz-wasm")
    .then(() => new Promise<void>((resolve) => setTimeout(resolve, 0)))
    .catch(() => undefined);
  await Promise.all([worker, requested]);
}
