import type { WasmSchema } from "../drivers/types.js";
import type { JazzClient } from "./client.js";
import type { DbConfig } from "./db.js";
import type { AuthFailureReason } from "./auth-state.js";

export interface RuntimeTokenOptions {
  secret: string;
  audience: string;
  ttlSeconds: number;
  nowSeconds: bigint;
}

export interface CoreClientContext<RuntimeConfig extends DbConfig = DbConfig> {
  config: RuntimeConfig;
  schema: WasmSchema;
  onAuthFailure: (reason: AuthFailureReason) => void;
}

export interface CoreTelemetryContext<RuntimeConfig extends DbConfig = DbConfig> {
  config: RuntimeConfig;
  collectorUrl: string;
  runtimeThread: "main" | "worker";
}

/**
 * Internal source for loading and wiring the core runtime.
 *
 * This keeps platform/source differences (WASM, NAPI, browser storage, React
 * Native support status) out of Db without presenting alternate database
 * engines as a public extension point.
 */
export abstract class CoreSource<RuntimeConfig extends DbConfig = DbConfig> {
  /** Set to false when the core must receive schemas exactly as declared. */
  readonly supportsPolicyBypass: boolean = true;
  private hasLoadedCore = false;
  private loadedCoreValue: unknown;

  async load(config: RuntimeConfig): Promise<void> {
    if (this.hasLoadedCore) {
      return;
    }

    this.loadedCoreValue = await this.loadCore(config);
    this.hasLoadedCore = true;
  }

  protected abstract loadCore(config: RuntimeConfig): Promise<unknown>;

  protected get loadedCore(): unknown {
    if (!this.hasLoadedCore) {
      throw new Error("Db core source is not loaded");
    }
    return this.loadedCoreValue;
  }

  abstract createClient(context: CoreClientContext<RuntimeConfig>): JazzClient;

  installTelemetry(_context: CoreTelemetryContext<RuntimeConfig>): (() => void) | null | undefined {
    return null;
  }

  mintLocalFirstToken(_options: RuntimeTokenOptions): string {
    throw new Error("Db core source does not support local-first auth");
  }

  mintAnonymousToken(_options: RuntimeTokenOptions): string {
    throw new Error("Db core source does not support anonymous auth");
  }
}
