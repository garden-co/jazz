import "./abort-signal.js";
import { clientScope } from "../runtime/client-scope.js";
import { installNativeRuntimeEntropy } from "../runtime/runtime-entropy.js";
import type { RuntimeClientContext } from "../runtime/runtime-source.js";
import { RuntimeSource } from "../runtime/runtime-source.js";
import type { TextAnchors } from "../runtime/text-anchors.js";
import type { JazzClient } from "../runtime/client.js";
import { JazzClient as JazzRuntimeClient } from "../runtime/client.js";
import type { Session } from "../runtime/context.js";
import {
  markTrustedReservedSession,
  parseJwtPayload,
  internalSessionFromJwtPayload,
  internalSessionFromVerifiedReservedJwtPayload,
} from "../runtime/client-session.js";
import type { AppContext } from "../runtime/context.js";
import type { DbConfig } from "../runtime/db.js";
import { NativeRuntimeAdapter } from "../runtime/native-runtime/native-runtime-adapter.js";
import {
  getTrustedReservedSession,
  setTrustedReservedSession,
} from "../runtime/db-internal-session.js";
import { authorBytesForSession } from "../runtime/author-id.js";
import { accountConfigBearer, assertAccountConfig } from "../accounts/config-capability.js";
import { askBearer } from "../runtime/credentialed-transport.js";
import {
  beginNativeAccountSession,
  attachNativeAccountSchema,
  resolveNativeSession,
} from "./account-session.js";
import { serializeSchemaSource } from "../drivers/schema-wire.js";
import type { WasmSchema } from "../drivers/types.js";
import { shippedMigrations, type ClientMigrations } from "../runtime/client-migrations.js";
import { resolveSchemaSource } from "../schema-source.js";
import {
  REACT_NATIVE_MEMORY_RUNTIME_UNSUPPORTED_ERROR,
  REACT_NATIVE_NATIVE_RELAY_MEMORY_ONLY_ERROR,
  REACT_NATIVE_NATIVE_RELAY_REQUIRED_ERROR,
} from "./errors.js";
import {
  NativeForegroundDb,
  type NativeForegroundFactory,
  type NativeForegroundModule,
} from "./native-foreground-db.js";
import { importNativeRelay } from "./native-relay.js";
import { compiledBindings } from "../encryption/build.js";
import { declaresEncryption } from "../encryption/plain-schema.js";
import { Encryption, EncryptionStoreRequiredError } from "../encryption/index.js";
import { servePrivateStore } from "../encryption/private-store.js";

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
   * Opaque authority issued by trusted native platform admission. The normal
   * persistent RN runtime consumes it through its installed JSI foreground
   * engine; application JavaScript cannot construct scope configuration.
   */
  nativeRelay?: ReactNativeRelayConfig;
};

function shouldRequireSqliteDriver(config: ReactNativeDbConfig): boolean {
  return (config.driver?.type ?? "persistent") === "persistent";
}

export class ReactNativeRuntimeSource extends RuntimeSource<ReactNativeDbConfig> {
  private admittedSession: Session | null = null;
  private admittedCapability: Uint8Array | null = null;
  private foregroundModule: NativeForegroundModule | null = null;
  private foregroundFactory: NativeForegroundFactory | null = null;
  private lifecycleForeground: NativeForegroundDb | null = null;
  private encryptionSchema?: WasmSchema;
  private encryptionStore?: Pick<ReturnType<typeof servePrivateStore>, "stop" | "onStateChange">;
  private adapter?: NativeRuntimeAdapter;
  private pendingCapability: Uint8Array | null = null;
  private ownsAdmission = false;
  private explicitlyOffline = false;

  override textAnchors(): TextAnchors {
    const call = this.foregroundFactory?.textAnchors;
    if (!call) throw new Error("This React Native runtime does not support text anchors");
    const run = (request: Record<string, unknown>) => JSON.parse(call(JSON.stringify(request)));
    return {
      captureAnchor: (text, position, side, version) =>
        run({ op: "captureAnchor", text, position, side, version }),
      resolveAnchor: (anchor, text, version) => run({ op: "resolveAnchor", anchor, text, version }),
      captureRange: (text, start, end, version) =>
        run({ op: "captureRange", text, start, end, version }),
      resolveRange: (range, text, version) => run({ op: "resolveRange", range, text, version }),
    };
  }

  override async load(config: ReactNativeDbConfig): Promise<void> {
    if (shouldRequireSqliteDriver(config)) {
      const encryption =
        config.encryption && declaresEncryption(config.encryption.app)
          ? config.encryption
          : undefined;
      const store = encryption?.store;
      if (encryption && !store) throw new EncryptionStoreRequiredError();
      if (encryption && config.nativeRelay) {
        throw new Error("React Native encryption requires an account opened by this client");
      }
      if (!config.nativeRelay && config.accountId && config.accountRegistryAuthority) {
        if (this.pendingCapability || this.admittedCapability) return;
        assertAccountConfig(config);
        const session = resolveNativeSession(config);
        const module = await importNativeRelay();
        const factory = module.installNativeForegroundRuntime();
        if (factory.accountSecret) installNativeRuntimeEntropy(factory.accountSecret);
        const bearer = config.cookieSession
          ? ((await accountConfigBearer(config)?.()) ?? undefined)
          : undefined;
        this.pendingCapability = beginNativeAccountSession(
          factory,
          config,
          session,
          bearer,
          encryption !== undefined,
        );
        this.foregroundFactory = factory;
        this.foregroundModule = module;
        this.ownsAdmission = true;
        this.admittedSession = markTrustedReservedSession({
          ...session,
          account_id: config.accountId,
        });
        this.nativeConnection = {
          configured: () => config.serverUrl !== undefined,
          disconnect: () => {
            this.explicitlyOffline = true;
            this.lifecycleForeground?.disconnectNativeUpstream();
          },
          reconnect: () => this.reconnectOwnedUpstream(config),
        };
        if (encryption && store) {
          const schema = resolveSchemaSource(encryption.app);
          this.encryptionSchema = schema;
          this.attachSchema(
            schema,
            shippedMigrations(schema),
            compiledBindings(encryption.app),
            encryption.addCreatorAsMember,
          );
          const foreground = new NativeForegroundDb(
            factory.openAttached(this.admittedCapability!),
            module,
          );
          this.lifecycleForeground = foreground;
          this.assertForegroundIdentity(foreground, config, session);
          const serving = servePrivateStore(foreground, store);
          this.encryptionStore = {
            stop: () => serving.stop({ cancelPending: true }),
            onStateChange: serving.onStateChange,
          };
          await foreground.openEncryption();
        }
        return;
      }
      if (config.nativeRelay) {
        if (this.admittedCapability) return;
        assertNativeRelay(config.nativeRelay);
        // Capture before the first await: caller-owned bytes must never select
        // a different scope after native identity preflight.
        const capability = new Uint8Array(config.nativeRelay.capability);
        const foreground = await importNativeRelay();
        this.foregroundFactory = foreground.installNativeForegroundRuntime();
        if (this.foregroundFactory.accountSecret)
          installNativeRuntimeEntropy(this.foregroundFactory.accountSecret);
        this.foregroundModule = foreground;
        const withForeground = <T>(run: (db: NativeForegroundDb) => T): T => {
          if (this.lifecycleForeground) return run(this.lifecycleForeground);
          // Connectivity is available before the lazy schema client exists.
          const temporary = new NativeForegroundDb(
            this.foregroundFactory!.openAttached(capability),
            foreground,
          );
          try {
            return run(temporary);
          } finally {
            temporary.close();
          }
        };
        const opened = new NativeForegroundDb(
          this.foregroundFactory.openAttached(capability),
          foreground,
        );
        try {
          const selected = resolveNativeSession(config);
          const metadata = this.assertForegroundIdentity(opened, config, selected);
          const configured = opened.nativeConnectionStatus().configured;
          // The admitted scope's transport configuration is immutable. Lifecycle
          // actions use the application owner, rather than opening a recovery
          // foreground merely to inspect or change connectivity.
          this.nativeConnection = {
            configured: () => configured,
            disconnect: () => withForeground((db) => db.disconnectNativeUpstream()),
            reconnect: () => withForeground((db) => db.reconnectNativeUpstream()),
          };
          this.admittedSession = markTrustedReservedSession({
            account_id: metadata.accountId ?? undefined,
            issuer: metadata.issuer,
            user_id: metadata.userId,
            claims: selected.claims,
            authMode:
              metadata.issuer === "urn:jazz:local-first"
                ? "device-account"
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
    if (this.ownsAdmission) return;
    // The handle's JWT was checked against the native account/identity during
    // load. Keep its bearer representation: reserved local-first identities
    // deliberately cannot enter through the generic cookie-session path.
    // Native authority still comes exclusively from the admitted capability.
    setTrustedReservedSession(config, this.admittedSession);
  }

  private attachSchema(
    schema: WasmSchema,
    migrations?: ClientMigrations,
    bindings?: string,
    addCreatorAsMember?: boolean,
  ): void {
    const pending = this.pendingCapability!;
    const serialized = serializeSchemaSource(schema, migrations, bindings, addCreatorAsMember);
    try {
      this.admittedCapability = attachNativeAccountSchema(
        this.foregroundFactory!,
        pending,
        serialized,
      );
    } finally {
      this.pendingCapability = null;
    }
  }

  private assertForegroundIdentity(
    foreground: NativeForegroundDb,
    config: ReactNativeDbConfig,
    session: Session,
  ): ReturnType<NativeForegroundDb["nativeSessionMetadata"]> {
    const metadata = foreground.nativeSessionMetadata();
    if (
      metadata.accountId !== (config.accountId ?? null) ||
      metadata.issuer !== session.issuer ||
      metadata.userId !== session.user_id ||
      (config.accountRegistryAuthority !== undefined &&
        metadata.registryAuthority !== config.accountRegistryAuthority)
    ) {
      throw new Error("React Native admitted session does not match the selected account identity");
    }
    return metadata;
  }

  override createClient(context: RuntimeClientContext<ReactNativeDbConfig>): JazzClient {
    if (this.encryptionSchema && context.schema !== this.encryptionSchema) {
      throw new Error("React Native encrypted client was opened with a different generated app");
    }
    if (!this.encryptionSchema && declaresEncryption(context.schema)) {
      throw new EncryptionStoreRequiredError();
    }
    if (this.pendingCapability) this.attachSchema(context.schema, context.migrations);
    if (this.admittedCapability) {
      const factory = this.foregroundFactory;
      const module = this.foregroundModule;
      const capability = this.admittedCapability;
      if (!factory || !module)
        throw new Error("React Native native foreground runtime is not loaded");
      const session = resolveNativeSession(context.config);
      const foreground = this.encryptionSchema
        ? this.lifecycleForeground
        : new NativeForegroundDb(factory.openAttached(capability), module);
      if (!foreground) throw new Error("React Native encrypted foreground runtime is not loaded");
      try {
        const metadata = this.assertForegroundIdentity(foreground, context.config, session);
        if (this.explicitlyOffline) foreground.disconnectNativeUpstream();
        this.lifecycleForeground = foreground;
        const runtime = NativeRuntimeAdapter.fromDb(
          foreground,
          context.schema,
          metadata.node,
          authorBytesForSession(session),
          1,
          false,
          {
            slowQueryLog: {
              appId: clientScope(context.config),
              thresholdMs: context.config.slowQueryThresholdMs,
            },
          },
        );
        if (this.encryptionStore) {
          this.adapter = runtime;
          const deviceAccount = session.authMode === "device-account";
          runtime.attachEncryption(
            foreground,
            this.encryptionStore,
            (native, stateChanges) =>
              new Encryption(native, { nativeReadiness: true, deviceAccount, stateChanges }),
          );
        }
        const appContext: AppContext = {
          appId: clientScope(context.config),
          accountId: context.config.accountId,
          schema: context.schema,
          driver: context.config.driver,
          serverUrl: context.config.serverUrl,
          env: context.config.env,
          jwtToken: context.config.jwtToken,
          cookieSession: context.config.cookieSession,
        };
        setTrustedReservedSession(appContext, getTrustedReservedSession(context.config));
        return JazzRuntimeClient.connectWithRuntime(runtime, appContext, {
          onAuthFailure: context.onAuthFailure,
          onAuthRestored: context.onAuthRestored,
        });
      } catch (error) {
        this.lifecycleForeground = null;
        foreground.close();
        throw error;
      }
    }
    throw new Error(REACT_NATIVE_MEMORY_RUNTIME_UNSUPPORTED_ERROR);
  }

  override encryptionHandle(): Encryption | undefined {
    return this.adapter?.encryptionHandle();
  }

  override async shutdown(): Promise<void> {
    if (!this.ownsAdmission) return;
    const capability = this.admittedCapability ?? this.pendingCapability;
    const foreground = this.lifecycleForeground;
    const adapter = this.adapter;
    const store = this.encryptionStore;
    const encrypted = this.encryptionSchema !== undefined;
    this.admittedCapability = null;
    this.pendingCapability = null;
    this.lifecycleForeground = null;
    this.adapter = undefined;
    this.encryptionSchema = undefined;
    this.encryptionStore = undefined;
    this.ownsAdmission = false;
    try {
      if (encrypted) {
        store?.stop();
        try {
          await adapter?.close();
        } finally {
          foreground?.close();
        }
      }
    } finally {
      if (capability) this.foregroundFactory!.releaseAccountSession!(capability);
    }
  }

  /**
   * Reconnect the upstream of a scope this source admitted. A worker whose
   * credential the sync server refused is handed the app's bearer first: it
   * would only be refused again with the one it has. Any other reconnect goes
   * straight through, without waiting on the app.
   */
  private async reconnectOwnedUpstream(config: ReactNativeDbConfig): Promise<void> {
    this.explicitlyOffline = false;
    if (this.credentialMayBeRefused()) await this.presentCurrentBearer(config);
    this.lifecycleForeground?.reconnectNativeUpstream();
  }

  /**
   * Whether the worker's credential was refused. Without a foreground to ask
   * (no client opened yet) it may have been.
   */
  private credentialMayBeRefused(): boolean {
    const foreground = this.lifecycleForeground;
    if (!foreground) return true;
    try {
      return Boolean(foreground.nativeConnectionStatus().authRejected);
    } catch {
      return true;
    }
  }

  /**
   * A user signed in through the app's backend syncs with the app's bearer,
   * which the native socket worker was handed when the account was admitted.
   * Once it expires the sync server refuses it, and a reconnect with the same
   * one is refused again: so a reconnect (the one `followUser` asks for after
   * a refusal, or the app's own) asks the app for its bearer again first, and
   * a different one replaces the worker's. The app's bearer failing leaves the
   * worker's as it was: the reconnect still happens.
   */
  private async presentCurrentBearer(config: ReactNativeDbConfig): Promise<void> {
    if (!config.cookieSession) return;
    const capability = this.admittedCapability ?? this.pendingCapability;
    if (!capability) return;
    const ask = accountConfigBearer(config);
    if (!ask) return;
    let bearer: string | null | undefined;
    try {
      // Bounded: this runs in the shared transport queue, so a bearer that
      // never settles would hold every later disconnect and reconnect.
      bearer = await askBearer(ask);
    } catch {
      return;
    }
    // Released or replaced while the app answered: not this worker's any more.
    if (!bearer || (this.admittedCapability ?? this.pendingCapability) !== capability) return;
    try {
      this.foregroundFactory!.refreshAccountSession!(
        capability,
        JSON.stringify({ bearer, claims: config.cookieSession.claims ?? {} }),
      );
    } catch {
      // The worker keeps the bearer it has; reconnecting with it is no worse
      // than before.
    }
  }

  override refreshAccountToken(token: string): boolean {
    if (!this.ownsAdmission) this.assertAuthUpdateAllowed();
    const capability = this.admittedCapability ?? this.pendingCapability;
    if (!capability) throw new Error("React Native account context is closed");
    const payload = parseJwtPayload(token);
    const session =
      payload?.iss === "urn:jazz:local-first"
        ? internalSessionFromVerifiedReservedJwtPayload(payload, "device-account")
        : payload && internalSessionFromJwtPayload(payload);
    if (!session) throw new Error("Invalid account refresh token");
    this.foregroundFactory!.refreshAccountSession!(
      capability,
      JSON.stringify({ jwt: token, claims: session.claims }),
    );
    return true;
  }
}

function assertNativeRelay(relay: NonNullable<ReactNativeDbConfig["nativeRelay"]>): void {
  if (!(relay.capability instanceof Uint8Array) || relay.capability.byteLength !== 32)
    throw new Error(REACT_NATIVE_NATIVE_RELAY_REQUIRED_ERROR);
}
