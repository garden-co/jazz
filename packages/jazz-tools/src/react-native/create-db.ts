import "./abort-signal.js";
import { Db } from "../runtime/db.js";
import { createAccountDbWithRuntimeSource, type AccountDbConfig } from "../accounts/context.js";
import { ReactNativeRuntimeSource, type ReactNativeDbConfig } from "./runtime-source.js";
import type { PrivateStore } from "../encryption/private-store.js";
import type { SchemaSourceInput } from "../schema-source.js";

export { Db };
export type DbConfig = Omit<AccountDbConfig, "encryption"> &
  Pick<ReactNativeDbConfig, "nativeRelay"> & {
    encryption?: {
      /** The app exported by the generated Jazz bindings. */
      app: SchemaSourceInput;
      /** The app's OS-protected store for this device's encryption keys. */
      store: PrivateStore;
    };
  };

export async function createDb(config: DbConfig): Promise<Db> {
  return createAccountDbWithRuntimeSource(config, new ReactNativeRuntimeSource());
}
