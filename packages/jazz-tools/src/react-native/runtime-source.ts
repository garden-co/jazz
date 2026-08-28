import { DefaultRuntimeSource } from "../runtime/default-runtime-source.js";
import type { RuntimeClientContext, RuntimeTokenOptions } from "../runtime/runtime-source.js";
import { RuntimeSource } from "../runtime/runtime-source.js";
import type { JazzClient } from "../runtime/client.js";
import type { DbConfig } from "../runtime/db.js";
import type {
  DirectRuntimeConnection,
  DirectRuntimeConnectionContext,
} from "../runtime/runtime-source.js";
import { NativeRuntimeAdapter } from "../runtime/native-runtime/native-runtime-adapter.js";
import {
  getTrustedReservedSession,
  setTrustedReservedSession,
} from "../runtime/db-internal-session.js";
import type { ReactNativeSqliteStorageDriver } from "./storage.js";
import {
  ReactNativeRelayFrameAdapter,
  type NativeRelayCapability,
  type NativeRelayExecutor,
} from "./native-relay-frame-adapter.js";
import { REACT_NATIVE_SQLITE_STORAGE_REJECTED_ERROR } from "./storage.js";

export type ReactNativeDbConfig = DbConfig & {
  /**
   * Proposal-only SQLite storage hook for a future native v2 runtime.
   *
   * The current runtime does not install or open this driver. Every persistent
   * configuration is rejected before `sqliteStorage.open()` can run. Supplying
   * it with an explicit memory driver is also rejected rather than ignored.
   *
   * @deprecated Ignored and rejected; do not supply this option until the
   * native ordered-KV runtime exists.
   */
  sqliteStorage?: ReactNativeSqliteStorageDriver;
  /**
   * Opaque authority issued by the installed Android/iOS JazzRelay artifact.
   *
   * The capability admits this foreground peer to its native durable relay;
   * it is not a database path, schema, session, token, or a way to derive any
   * of those values in JavaScript.
   */
  nativeRelay?: Readonly<{
    executor: NativeRelayExecutor;
    capability: NativeRelayCapability;
  }>;
};

export const REACT_NATIVE_NATIVE_RELAY_REQUIRED_ERROR =
  "React Native persistent runtime requires the installed JazzRelay native artifact and its platform-provided opaque nativeRelay capability";

export const REACT_NATIVE_NATIVE_RELAY_MEMORY_ONLY_ERROR =
  "React Native nativeRelay is only valid with persistent storage; remove nativeRelay when driver.type='memory'";

function shouldRequireSqliteDriver(config: ReactNativeDbConfig): boolean {
  return (config.driver?.type ?? "persistent") === "persistent";
}

export class ReactNativeRuntimeSource extends RuntimeSource<ReactNativeDbConfig> {
  private readonly fallback = new DefaultRuntimeSource();

  override async load(config: ReactNativeDbConfig): Promise<void> {
    if (config.sqliteStorage !== undefined) {
      throw new Error(REACT_NATIVE_SQLITE_STORAGE_REJECTED_ERROR);
    }
    if (shouldRequireSqliteDriver(config)) {
      if (!config.nativeRelay) throw new Error(REACT_NATIVE_NATIVE_RELAY_REQUIRED_ERROR);
      assertNativeRelay(config.nativeRelay);
      // The foreground runtime is intentionally in-memory. Its sole durable
      // peer is the platform-owned relay, attached below through canonical
      // frames; no SQLite/path/schema/session/token crosses this boundary.
      await this.fallback.load(foregroundConfig(config));
      return;
    }

    if (config.nativeRelay) throw new Error(REACT_NATIVE_NATIVE_RELAY_MEMORY_ONLY_ERROR);

    await this.fallback.load(config);
  }

  override createClient(context: RuntimeClientContext<ReactNativeDbConfig>): JazzClient {
    return this.fallback.createClient({ ...context, config: foregroundConfig(context.config) });
  }

  override createDirectConnection(
    context: DirectRuntimeConnectionContext<ReactNativeDbConfig>,
  ): DirectRuntimeConnection | null {
    const relay = context.config.nativeRelay;
    if (!relay) return null;
    assertNativeRelay(relay);
    const runtime = context.client.getRuntime();
    if (!(runtime instanceof NativeRuntimeAdapter)) {
      throw new Error(
        "React Native JazzRelay requires a compatible native foreground runtime; the installed runtime artifact is missing or incompatible",
      );
    }
    return new ReactNativeRelayConnection(runtime, relay);
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

class ReactNativeRelayConnection implements DirectRuntimeConnection {
  private adapter: ReactNativeRelayFrameAdapter | null = null;
  private readyPromise: Promise<void> | null = null;
  private closed = false;

  constructor(
    private readonly runtime: NativeRuntimeAdapter,
    private readonly relay: NonNullable<ReactNativeDbConfig["nativeRelay"]>,
  ) {}

  async ready(): Promise<void> {
    if (this.closed) throw new Error("React Native JazzRelay connection is shut down");
    this.readyPromise ??= this.start();
    await this.readyPromise;
  }

  async disconnect(): Promise<void> {
    const adapter = this.adapter;
    this.adapter = null;
    this.readyPromise = null;
    await adapter?.shutdown();
  }

  async reconnect(): Promise<void> {
    await this.disconnect();
    await this.ready();
  }

  async shutdown(): Promise<void> {
    this.closed = true;
    await this.disconnect();
  }

  private async start(): Promise<void> {
    const adapter = new ReactNativeRelayFrameAdapter(
      this.runtime,
      this.runtime.connectUpstreamPeer(),
      this.relay.executor,
      this.relay.capability,
      reportRelayProgressError,
    );
    this.adapter = adapter;
    try {
      await adapter.start();
    } catch (error) {
      if (this.adapter === adapter) this.adapter = null;
      await adapter.shutdown();
      const detail = error instanceof Error ? error.message : String(error);
      throw new Error(
        `React Native JazzRelay startup failed; verify the installed native artifact, platform admission capability, and relay command ABI: ${detail}`,
      );
    }
  }
}

function reportRelayProgressError(error: Error): void {
  // A frame-progress error is deliberately non-terminal here. The adapter
  // retains its FIFO frame and a later native work notification can resume it
  // after transient backpressure. Startup/ABI/admission failures still reject
  // ready() with the contextual error above.
  console.error("React Native JazzRelay frame progress failed", error);
}

function foregroundConfig(config: ReactNativeDbConfig): DbConfig {
  if (!shouldRequireSqliteDriver(config)) return config;
  const foreground = { ...config, driver: { type: "memory" as const } };
  // The verified session is a package-private sidecar, not part of the relay
  // configuration or relay command ABI. Preserve it for the ordinary
  // foreground runtime without making it enumerable to application code.
  setTrustedReservedSession(foreground, getTrustedReservedSession(config));
  return foreground;
}

function assertNativeRelay(
  relay: NonNullable<ReactNativeDbConfig["nativeRelay"]>,
): asserts relay is NonNullable<ReactNativeDbConfig["nativeRelay"]> {
  if (!relay || typeof relay.executor?.execute !== "function") {
    throw new Error(REACT_NATIVE_NATIVE_RELAY_REQUIRED_ERROR);
  }
  if (!(relay.capability instanceof Uint8Array) || relay.capability.byteLength !== 32) {
    throw new Error(
      "React Native JazzRelay requires a 32-byte platform-provided opaque capability",
    );
  }
}
