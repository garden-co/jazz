import { createDbWithCoreSource, Db } from "../runtime/db.js";
import { ReactNativeCoreSource, type ReactNativeRuntimeDbConfig } from "./runtime-module.js";

export { Db };

export interface DbConfig extends ReactNativeRuntimeDbConfig {}

export async function createDb(config: DbConfig): Promise<Db> {
  return await createDbWithCoreSource(config, new ReactNativeCoreSource());
}
