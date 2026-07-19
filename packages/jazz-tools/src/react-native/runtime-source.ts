import { DefaultRuntimeSource } from "../runtime/default-runtime-source.js";
import type { RuntimeClientContext, RuntimeTokenOptions } from "../runtime/runtime-source.js";
import { RuntimeSource } from "../runtime/runtime-source.js";
import type { JazzClient } from "../runtime/client.js";
import type { DbConfig } from "../runtime/db.js";
import { resolveDefaultPersistentDbName } from "../runtime/db.js";
import type { ReactNativeSqliteStorageDriver } from "./storage.js";
import { UnimplementedSqliteStorageDriver } from "./storage.js";

export interface ReactNativeDbConfig extends DbConfig {
  /**
   * Future SQLite storage driver hook for React Native persistence.
   *
   * The current scaffold typechecks only. The default driver deliberately
   * throws a clear implementation-pending error before opening the runtime.
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
      const driver = config.sqliteStorage ?? new UnimplementedSqliteStorageDriver();
      await driver.open(resolveDefaultPersistentDbName(config));
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
