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
import { explainingSecretFormat, secretFormatExplained } from "./secret-format.js";

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
  ): StoreTarget & { ready(): Promise<unknown>; setAddCreatorAsMember?(enabled: boolean): void };
}

/**
 * Builds the native runtime for an app that declares encryption.
 * Without a store this throws {@link EncryptionStoreRequiredError}.
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
  addCreatorAsMember?: boolean;
}): Promise<{ runtime: StoreTarget; store: PrivateStore }> {
  const store = options.store;
  if (!store) throw new EncryptionStoreRequiredError();
  const runtime = explainingSecretFormat(() =>
    options.host.fromBindings(
      compiledBindings(options.app),
      options.appId,
      options.environment,
      options.accountId,
      options.deviceAccountSecret,
    ),
  );
  if (options.addCreatorAsMember === false) {
    if (!runtime.setAddCreatorAsMember) {
      throw new Error("This encryption runtime does not support the creator membership option.");
    }
    runtime.setAddCreatorAsMember(false);
  }
  // The store answers the runtime while it loads or makes the device.
  const serving = servePrivateStore(runtime, store);
  try {
    await runtime.ready();
  } catch (error) {
    throw secretFormatExplained(error);
  } finally {
    serving.stop();
  }
  return { runtime, store };
}
