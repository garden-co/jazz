/**
 * Uses the engine installed by `backendNative` for Node device clients
 * and for backends that manage members without holding keys.
 */
import { backendNative } from "../backend/backend-native.js";
import type { WasmSchema } from "../drivers/types.js";
import type { Db } from "../runtime/db.js";
import type { NativeRuntimeAdapter } from "../runtime/native-runtime/native-runtime-adapter.js";
import type { SchemaSourceInput } from "../schema-source.js";
import { buildEncryption, compiledBindings } from "./build.js";
import { declaresEncryption } from "./plain-schema.js";
import {
  Devices,
  EncryptionError,
  Keys,
  Members,
  type Encryption,
  type Recovery,
} from "./index.js";
import type { PrivateStore } from "./private-store.js";

export { declaresEncryption };

/** What an app passes to turn its encryption on: where the device's private keys live. */
export interface NodeEncryptionConfig {
  /** The private store (see {@link PrivateStore}) holding this device's keys. */
  store: PrivateStore;
}

/**
 * The native runtime for `app` and `accountId`, ready, or `undefined` for an
 * app that declares no encryption. An app that declares it needs the store:
 * without one this throws `EncryptionStoreRequiredError`.
 */
export async function buildNodeEncryption(options: {
  app: SchemaSourceInput;
  appId: string;
  environment: string;
  accountId: string;
  /** The device account's secret (`jazz-auth-v1:...`); it is the account's recovery secret. */
  deviceAccountSecret?: string;
  encryption: NodeEncryptionConfig | undefined;
}): Promise<{ runtime: unknown; store: PrivateStore } | undefined> {
  if (!declaresEncryption(options.app)) return undefined;
  const host = backendNative().EncryptionRuntime;
  if (!host) {
    throw new EncryptionError(
      "unsupported",
      "A backend client of an app that declares end-to-end encryption is not supported in a Cloudflare Worker yet",
    );
  }
  return buildEncryption({
    host,
    app: options.app,
    appId: options.appId,
    environment: options.environment,
    accountId: options.accountId,
    deviceAccountSecret: options.deviceAccountSecret,
    store: options.encryption?.store,
  });
}

/**
 * What a backend opens with for an app that declares encryption: the
 * bindings' compiled app, from which it builds keyless hooks. `undefined`
 * for an app that declares none.
 */
export function keylessBackend(
  app: SchemaSourceInput,
  appId: string,
  environment: string,
): { bindings: string; appId: string; environment: string } | undefined {
  if (!declaresEncryption(app)) return undefined;
  return { bindings: compiledBindings(app), appId, environment };
}

/** Member management on the backend's own database, without a device or private store. */
export function keylessEncryption(
  db: Db,
  schema: WasmSchema,
  runtime: NativeRuntimeAdapter,
): Encryption {
  const refuse = (): never => {
    throw new EncryptionError(
      "keyless",
      "The backend holds no encryption keys. Use a device client for this operation.",
    );
  };
  const reject = async (): Promise<never> => refuse();
  const recovery = {
    create: refuse,
    enable: reject,
    restoreFromPasskey: reject,
    register: reject,
    restore: reject,
    list: reject,
    retire: reject,
  } satisfies Pick<Recovery, keyof Recovery>;
  const members = new Members(
    async ({ table, id }) => {
      try {
        return { scopeId: runtime.tableIdentity(table), identifier: id };
      } catch (error) {
        if (
          error instanceof Error &&
          (("code" in error && error.code === "not_observed") ||
            error.message.includes("NotObserved"))
        ) {
          throw new EncryptionError("not-observed", error.message);
        }
        throw error;
      }
    },
    { db, schema },
  );
  const handle = {
    members,
    devices: new Devices(reject),
    keys: new Keys(reject),
    recovery,
    deviceId: reject,
    resetAccount: reject,
    ready: reject,
    readable: reject,
    start: refuse,
    stop: refuse,
    onReport: refuse,
    onUnreadable: refuse,
    onChallenge: refuse,
  } satisfies Omit<Pick<Encryption, keyof Encryption>, "recovery"> & {
    recovery: typeof recovery;
  };
  // The public methods match Encryption without constructing a device runtime.
  return handle as unknown as Encryption;
}
