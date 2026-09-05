import type { RuntimeClientContext } from "../runtime/runtime-source.js";
import { RuntimeSource } from "../runtime/runtime-source.js";
import type { JazzClient } from "../runtime/client.js";
import { JazzClient as JazzRuntimeClient } from "../runtime/client.js";
import type { Session } from "../runtime/context.js";
import { markTrustedReservedSession } from "../runtime/client-session.js";
import type { AppContext } from "../runtime/context.js";
import type { DbConfig } from "../runtime/db.js";
import { NativeRuntimeAdapter } from "../runtime/native-runtime/native-runtime-adapter.js";
import {
  getTrustedReservedSession,
  setTrustedReservedSession,
} from "../runtime/db-internal-session.js";
import { resolveClientInternalSessionSync } from "../runtime/client-session.js";
import { authorBytesForSession } from "../runtime/author-id.js";
import type { ReactNativeSqliteStorageDriver } from "./storage.js";
import { REACT_NATIVE_SQLITE_STORAGE_REJECTED_ERROR } from "./storage.js";
import {
  NativeForegroundDb,
  type NativeForegroundFactory,
  type NativeForegroundModule,
} from "./native-foreground-db.js";

/**
 * Opaque authority supplied by trusted native platform admission.  It is
 * deliberately only a field of the React-Native client configuration: app
 * code must neither construct a scope nor use low-level foreground helpers.
 */
export type ReactNativeRelayConfig = Readonly<{
  capability: Uint8Array;
}>;

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
   * Opaque authority issued by trusted native platform admission. The normal
   * persistent RN runtime consumes it through its installed JSI foreground
   * engine; application JavaScript cannot construct scope configuration.
   */
  nativeRelay?: ReactNativeRelayConfig;
};

export const REACT_NATIVE_NATIVE_RELAY_REQUIRED_ERROR =
  "React Native persistent runtime requires the installed JazzRelay native artifact and its platform-provided opaque nativeRelay capability";
export const REACT_NATIVE_NATIVE_RELAY_MEMORY_ONLY_ERROR =
  "React Native nativeRelay is only valid with persistent storage; remove nativeRelay when driver.type='memory'";
export const REACT_NATIVE_MEMORY_RUNTIME_UNSUPPORTED_ERROR =
  "React Native requires the installed JazzRelay native runtime; driver.type='memory' is not supported";

function shouldRequireSqliteDriver(config: ReactNativeDbConfig): boolean {
  return (config.driver?.type ?? "persistent") === "persistent";
}

export class ReactNativeRuntimeSource extends RuntimeSource<ReactNativeDbConfig> {
  private admittedSession: Session | null = null;
  private admittedCapability: Uint8Array | null = null;
  private foregroundModule: NativeForegroundModule | null = null;
  private foregroundFactory: NativeForegroundFactory | null = null;

  override async load(config: ReactNativeDbConfig): Promise<void> {
    if (config.sqliteStorage !== undefined) {
      throw new Error(REACT_NATIVE_SQLITE_STORAGE_REJECTED_ERROR);
    }
    if (shouldRequireSqliteDriver(config)) {
      if (config.nativeRelay) {
        if (this.admittedCapability) return;
        assertNativeRelay(config.nativeRelay);
        // Capture before the first await: caller-owned bytes must never select
        // a different scope after native identity preflight.
        const capability = new Uint8Array(config.nativeRelay.capability);
        const foreground = (await import("jazz-rn/relay")) as unknown as NativeForegroundModule;
        this.foregroundFactory = foreground.installNativeForegroundRuntime();
        this.foregroundModule = foreground;
        const withForeground = <T>(run: (db: NativeForegroundDb) => T): T => {
          const db = new NativeForegroundDb(
            this.foregroundFactory!.openAttached(capability),
            foreground,
          );
          try {
            return run(db);
          } finally {
            db.close();
          }
        };
        this.nativeConnection = {
          configured: () => withForeground((db) => db.nativeConnectionStatus().configured),
          disconnect: () => withForeground((db) => db.disconnectNativeUpstream()),
          reconnect: () => withForeground((db) => db.reconnectNativeUpstream()),
        };
        const opened = new NativeForegroundDb(
          this.foregroundFactory.openAttached(capability),
          foreground,
        );
        try {
          const metadata = opened.nativeSessionMetadata();
          this.admittedSession = markTrustedReservedSession({
            issuer: metadata.issuer,
            user_id: metadata.userId,
            claims: {},
            authMode:
              metadata.issuer === "urn:jazz:local-first"
                ? "local-first"
                : metadata.issuer === "urn:jazz:anonymous"
                  ? "anonymous"
                  : "external",
          });
        } finally {
          opened.close();
        }
        this.admittedCapability = capability;
        return;
      }
      // A ReactNativeSqliteStorageDriver cannot yet be installed into the v2
      // Rust ordered-KV runtime. Opening one here and then delegating to WASM
      // only preflights an unrelated database and falsely implies that Jazz
      // rows are persisted there.
      throw new Error(REACT_NATIVE_NATIVE_RELAY_REQUIRED_ERROR);
    }

    if (config.nativeRelay) throw new Error(REACT_NATIVE_NATIVE_RELAY_MEMORY_ONLY_ERROR);
    // Do not delegate to DefaultRuntimeSource here. It is the browser/Node
    // implementation and imports the WASM runtime; Metro resolves that import
    // even for code paths that would never execute on a native device.
    throw new Error(REACT_NATIVE_MEMORY_RUNTIME_UNSUPPORTED_ERROR);
  }

  override assertAuthUpdateAllowed(): never {
    throw new Error(
      "React Native authentication is native-admission bound; revoke the old native scope, admit the new scope, and create a new Db",
    );
  }

  override admitConfig(config: ReactNativeDbConfig): void {
    if (!this.admittedSession) throw new Error("React Native native session is not admitted");
    // Public identity is derived from the native admission. Caller metadata
    // neither chooses authorization nor overrides the displayed identity.
    delete config.jwtToken;
    delete config.secret;
    delete config.adminSecret;
    config.cookieSession = this.admittedSession;
    setTrustedReservedSession(config, this.admittedSession);
  }

  override createClient(context: RuntimeClientContext<ReactNativeDbConfig>): JazzClient {
    if (this.admittedCapability) {
      const factory = this.foregroundFactory;
      const module = this.foregroundModule;
      const capability = this.admittedCapability;
      if (!factory || !module)
        throw new Error("React Native native foreground runtime is not loaded");
      const session = resolveNativeSession(context.config);
      const runtime = NativeRuntimeAdapter.fromDb(
        new NativeForegroundDb(factory.openAttached(capability), module),
        context.schema,
        randomNativeNodeBytes(),
        authorBytesForSession(session),
        1,
        false,
      );
      const appContext: AppContext = {
        appId: context.config.appId,
        schema: context.schema,
        driver: context.config.driver,
        serverUrl: context.config.serverUrl,
        env: context.config.env,
        jwtToken: context.config.jwtToken,
        cookieSession: context.config.cookieSession,
        tier: "local",
      };
      setTrustedReservedSession(appContext, getTrustedReservedSession(context.config));
      return JazzRuntimeClient.connectWithRuntime(runtime, appContext, {
        onAuthFailure: context.onAuthFailure,
      });
    }
    throw new Error(REACT_NATIVE_MEMORY_RUNTIME_UNSUPPORTED_ERROR);
  }
}

function assertNativeRelay(relay: NonNullable<ReactNativeDbConfig["nativeRelay"]>): void {
  if (!(relay.capability instanceof Uint8Array) || relay.capability.byteLength !== 32)
    throw new Error(REACT_NATIVE_NATIVE_RELAY_REQUIRED_ERROR);
}

function resolveNativeSession(config: ReactNativeDbConfig) {
  const session = resolveClientInternalSessionSync({
    ...config,
    trustedReservedSession: getTrustedReservedSession(config),
  });
  if (!session)
    throw new Error(
      "React Native native foreground requires an already verified jwtToken or cookieSession; native token minting is not implemented",
    );
  return session;
}

function randomNativeNodeBytes(): Uint8Array {
  const bytes = new Uint8Array(16);
  if (globalThis.crypto?.getRandomValues) return globalThis.crypto.getRandomValues(bytes);
  for (let index = 0; index < bytes.length; index += 1)
    bytes[index] = Math.floor(Math.random() * 256);
  return bytes;
}
