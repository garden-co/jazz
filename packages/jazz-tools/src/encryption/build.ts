/**
 * Builds the end-to-end encryption runtime of an app, the same on every host
 * that has the runtime (Node's native module, the browser's WebAssembly
 * module): from the declarations in the app's generated bindings, over the
 * private store the app (or the host's built-in store) supplies, for the
 * account that opens the database.
 */
import { resolveSchemaSource, type SchemaSourceInput } from "../schema-source.js";
import { boundApp } from "../typed-app.js";
import { EncryptionStoreRequiredError } from "./index.js";
import { servePrivateStore, type PrivateStore, type StoreTarget } from "./private-store.js";

interface Declared {
  schema: {
    current: string;
    encryption?: { versions?: Record<string, object> };
    schemas?: { hash: string; schema: { tables?: Record<string, unknown> } }[];
  };
}

/**
 * Whether the app is an encrypted app: its current version declares
 * encryption, or keeps Jazz's system tables without declaring an encrypted
 * column (once added, the tables stay).
 */
export function declaresEncryption(app: SchemaSourceInput): boolean {
  const bound = boundApp(resolveSchemaSource(app)) as Declared | undefined;
  if (!bound) return false;
  const current = bound.schema.encryption?.versions?.[bound.schema.current];
  if (current !== undefined && Object.keys(current).length > 0) return true;
  const version = bound.schema.schemas?.find((entry) => entry.hash === bound.schema.current);
  return Object.keys(version?.schema.tables ?? {}).some((name) => name.startsWith("__e2ee_"));
}

/** The bindings' compiled app as the runtime reads it. */
export function compiledBindings(app: SchemaSourceInput): string {
  const bound = boundApp(resolveSchemaSource(app))!;
  return JSON.stringify({ schema: bound.schema, relations: bound.relations });
}

/** The native `EncryptionRuntime` class of a host. */
export interface EncryptionRuntimeClass {
  fromBindings(
    bindings: string,
    appId: string,
    environment: string,
    accountId: string,
    deviceAccountSecret?: string,
  ): {
    ready(): Promise<unknown>;
    takeStoreRequests(): string[];
    replyStore(id: string, ok: boolean, payload: string): boolean;
  };
}

/**
 * The native runtime for `app` and `accountId`, ready, or `undefined` for an
 * app that declares no encryption. An app that declares it needs the store:
 * without one this throws {@link EncryptionStoreRequiredError}.
 */
export async function buildEncryption(options: {
  host: EncryptionRuntimeClass;
  app: SchemaSourceInput;
  appId: string;
  environment: string;
  accountId: string;
  /** The device account's secret (`jazz-auth-v1:...`); it is the account's recovery secret. */
  deviceAccountSecret?: string;
  store: PrivateStore | undefined;
}): Promise<{ runtime: StoreTarget; store: PrivateStore } | undefined> {
  if (!declaresEncryption(options.app)) return undefined;
  const store = options.store;
  if (!store) throw new EncryptionStoreRequiredError();
  const runtime = options.host.fromBindings(
    compiledBindings(options.app),
    options.appId,
    options.environment,
    options.accountId,
    options.deviceAccountSecret,
  );
  // The store answers the runtime while it loads or makes the device.
  const serving = servePrivateStore(runtime, store);
  try {
    await runtime.ready();
  } finally {
    serving.stop();
  }
  return { runtime, store };
}
