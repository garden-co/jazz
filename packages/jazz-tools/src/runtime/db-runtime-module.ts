import type { WasmSchema } from "../drivers/types.js";
import type { JazzClient } from "./client.js";
import type { DbConfig } from "./db.js";
import type { AuthFailureReason } from "./sync-transport.js";

export interface RuntimeTokenOptions {
  secret: string;
  audience: string;
  ttlSeconds: number;
  nowSeconds: bigint;
}

export interface DbRuntimeClientContext<RuntimeConfig extends DbConfig = DbConfig> {
  config: RuntimeConfig;
  schema: WasmSchema;
  hasWorker: boolean;
  useBinaryEncoding: boolean;
  onAuthFailure: (reason: AuthFailureReason) => void;
  onBeforeLocalBatchWait?: (batchId: string) => Promise<void>;
  onRejectedBatchAcknowledged: (batchId: string) => void;
}

export interface DbRuntimeTelemetryContext<RuntimeConfig extends DbConfig = DbConfig> {
  config: RuntimeConfig;
  collectorUrl: string;
  runtimeThread: "main" | "worker";
}

export abstract class DbRuntimeModule<RuntimeConfig extends DbConfig = DbConfig> {
  /** Set to false for runtimes, such as React Native, that cannot use browser workers. */
  readonly supportsBrowserWorker: boolean = true;
  /** Set to false when the runtime must receive schemas exactly as declared. */
  readonly supportsPolicyBypass: boolean = true;
  private hasLoadedRuntime = false;
  private loadedRuntimeValue: unknown;

  async load(config: RuntimeConfig): Promise<void> {
    if (this.hasLoadedRuntime) {
      return;
    }

    this.loadedRuntimeValue = await this.loadRuntime(config);
    this.hasLoadedRuntime = true;
  }

  protected abstract loadRuntime(config: RuntimeConfig): Promise<unknown>;

  protected get loadedRuntime(): unknown {
    if (!this.hasLoadedRuntime) {
      throw new Error("Db runtime module is not loaded");
    }
    return this.loadedRuntimeValue;
  }

  abstract createClient(context: DbRuntimeClientContext<RuntimeConfig>): JazzClient;

  installTelemetry(
    _context: DbRuntimeTelemetryContext<RuntimeConfig>,
  ): (() => void) | null | undefined {
    return null;
  }

  mintLocalFirstToken(_options: RuntimeTokenOptions): string {
    throw new Error("Db runtime module does not support local-first auth");
  }

  mintAnonymousToken(_options: RuntimeTokenOptions): string {
    throw new Error("Db runtime module does not support anonymous auth");
  }
}
