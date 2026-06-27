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

export interface RuntimeClientContext<RuntimeConfig extends DbConfig = DbConfig> {
  config: RuntimeConfig;
  schema: WasmSchema;
  onAuthFailure: (reason: AuthFailureReason) => void;
}

export interface RuntimeTelemetryContext<RuntimeConfig extends DbConfig = DbConfig> {
  config: RuntimeConfig;
  collectorUrl: string;
  runtimeThread: "main" | "worker";
}

/**
 * Internal source for loading and wiring the native runtime.
 *
 * This keeps platform/source differences (WASM, NAPI, browser storage, React
 * Native support status) out of Db. The active database path is core-only:
 * implementations preload the runtime, then create JazzClient instances for
 * concrete schemas.
 */
export abstract class RuntimeSource<RuntimeConfig extends DbConfig = DbConfig> {
  /** Set to false when the core must receive schemas exactly as declared. */
  readonly supportsPolicyBypass: boolean = true;

  async load(config: RuntimeConfig): Promise<void> {
    await this.loadCore(config);
  }

  protected async loadCore(_config: RuntimeConfig): Promise<unknown> {
    return undefined;
  }

  abstract createClient(context: RuntimeClientContext<RuntimeConfig>): JazzClient;

  installTelemetry(
    _context: RuntimeTelemetryContext<RuntimeConfig>,
  ): (() => void) | null | undefined {
    return null;
  }

  mintLocalFirstToken(_options: RuntimeTokenOptions): string {
    throw new Error("Db core source does not support local-first auth");
  }

  mintAnonymousToken(_options: RuntimeTokenOptions): string {
    throw new Error("Db core source does not support anonymous auth");
  }
}
