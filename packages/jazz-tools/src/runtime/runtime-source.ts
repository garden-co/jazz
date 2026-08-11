import type { WasmSchema } from "../drivers/types.js";
import type { AppContext } from "./context.js";
import type { JazzClient } from "./client.js";
import type { DbConfig } from "./db.js";
import type { AuthFailureReason } from "./auth-state.js";

export interface RuntimeTokenOptions {
  secret: string;
  audience: string;
  ttlSeconds: number;
  nowSeconds: bigint;
}

/**
 * The token-minting surface every native runtime module exposes. WASM and the
 * React Native binding generate the same signature, so sources only have to
 * name their module rather than restate both methods.
 */
export interface RuntimeTokenModule {
  mintLocalFirstToken(
    secret: string,
    audience: string,
    ttlSeconds: number,
    nowSeconds: bigint,
  ): string;
  mintAnonymousToken(
    secret: string,
    audience: string,
    ttlSeconds: number,
    nowSeconds: bigint,
  ): string;
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
 * Native support status) out of Db. The active database path is native-runtime backed:
 * implementations preload the runtime, then create JazzClient instances for
 * concrete schemas.
 */
export abstract class RuntimeSource<RuntimeConfig extends DbConfig = DbConfig> {
  /** Set to false when the runtime must receive schemas exactly as declared. */
  readonly supportsPolicyBypass: boolean = true;

  async load(config: RuntimeConfig): Promise<void> {
    await this.loadRuntime(config);
  }

  protected async loadRuntime(_config: RuntimeConfig): Promise<unknown> {
    return undefined;
  }

  abstract createClient(context: RuntimeClientContext<RuntimeConfig>): JazzClient;

  installTelemetry(
    _context: RuntimeTelemetryContext<RuntimeConfig>,
  ): (() => void) | null | undefined {
    return null;
  }

  /**
   * The connect context every source passes to
   * {@link JazzClient.connectWithRuntime}. It is derived entirely from the
   * config, so sources differ only in the runtime they hand alongside it.
   */
  protected connectContext(config: RuntimeConfig, schema: WasmSchema): AppContext {
    return {
      appId: config.appId,
      schema,
      driver: config.driver,
      serverUrl: config.serverUrl,
      env: config.env,
      userBranch: config.userBranch,
      jwtToken: config.jwtToken,
      cookieSession: config.cookieSession,
      backendSecret: config.backendSecret,
      adminSecret: config.adminSecret,
      tier: "local",
    };
  }

  /** Override to enable token minting; the default source supports neither. */
  protected tokenModule(): RuntimeTokenModule | null {
    return null;
  }

  mintLocalFirstToken(options: RuntimeTokenOptions): string {
    const module = this.tokenModule();
    if (!module) {
      throw new Error("Db runtime source does not support local-first auth");
    }
    return module.mintLocalFirstToken(
      options.secret,
      options.audience,
      options.ttlSeconds,
      options.nowSeconds,
    );
  }

  mintAnonymousToken(options: RuntimeTokenOptions): string {
    const module = this.tokenModule();
    if (!module) {
      throw new Error("Db runtime source does not support anonymous auth");
    }
    return module.mintAnonymousToken(
      options.secret,
      options.audience,
      options.ttlSeconds,
      options.nowSeconds,
    );
  }
}
