import { DefaultRuntimeSource } from "../runtime/default-runtime-source.js";
import type { RuntimeClientContext, RuntimeTokenOptions } from "../runtime/runtime-source.js";
import { RuntimeSource } from "../runtime/runtime-source.js";
import type { JazzClient } from "../runtime/client.js";
import type { DbConfig } from "../runtime/db.js";
import type { ReactNativeSqliteStorageDriver } from "./storage.js";
import { REACT_NATIVE_PERSISTENT_RUNTIME_UNAVAILABLE_ERROR } from "./storage.js";

export interface ReactNativeDbConfig extends DbConfig {
  /**
   * Proposal-only SQLite storage hook for a future native v2 runtime.
   *
   * The current runtime does not install or open this driver. Every persistent
   * configuration is rejected before `sqliteStorage.open()` can run.
   *
   * @deprecated Ignored and rejected; do not supply this option until the
   * native ordered-KV runtime exists.
   */
  sqliteStorage?: ReactNativeSqliteStorageDriver;
}

function shouldRequireSqliteDriver(config: ReactNativeDbConfig): boolean {
  return (config.driver?.type ?? "persistent") === "persistent";
}

export class ReactNativeRuntimeSource extends RuntimeSource<ReactNativeDbConfig> {
  private readonly fallback = new DefaultRuntimeSource();

  override async load(config: ReactNativeDbConfig): Promise<void> {
    if (shouldRequireSqliteDriver(config)) {
      // A ReactNativeSqliteStorageDriver cannot yet be installed into the v2
      // Rust ordered-KV runtime. Opening one here and then delegating to WASM
      // only preflights an unrelated database and falsely implies that Jazz
      // rows are persisted there.
      throw new Error(REACT_NATIVE_PERSISTENT_RUNTIME_UNAVAILABLE_ERROR);
    }

    await this.fallback.load(config);
  }

  override createClient(context: RuntimeClientContext<ReactNativeDbConfig>): JazzClient {
    return this.fallback.createClient(context);
  }

  override installTelemetry(
    context: Parameters<DefaultRuntimeSource["installTelemetry"]>[0],
  ): (() => void) | null | undefined {
    return this.fallback.installTelemetry(context);
  }

  override mintLocalFirstToken(options: RuntimeTokenOptions): string {
    return this.fallback.mintLocalFirstToken(options);
  }

  override mintAnonymousToken(options: RuntimeTokenOptions): string {
    return this.fallback.mintAnonymousToken(options);
  }
}
