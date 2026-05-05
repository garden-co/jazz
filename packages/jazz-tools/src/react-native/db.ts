import { createDbWithRuntimeModule, Db } from "../runtime/db.js";
import { ReactNativeRuntimeModule, type ReactNativeRuntimeDbConfig } from "./runtime-module.js";

export { Db };

export interface DbConfig extends ReactNativeRuntimeDbConfig {}

export async function createDb(config: DbConfig): Promise<Db> {
  return await createDbWithRuntimeModule(config, new ReactNativeRuntimeModule());
}
