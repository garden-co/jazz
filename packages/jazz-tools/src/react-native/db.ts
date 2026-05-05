import { createDb as createCoreDb, Db } from "../runtime/db.js";
import { ReactNativeBackendModule, type ReactNativeBackendDbConfig } from "./backend-module.js";

export { Db };

export interface DbConfig extends ReactNativeBackendDbConfig {}

export async function createDb(config: DbConfig): Promise<Db> {
  return await createCoreDb({
    ...config,
    runtime: new ReactNativeBackendModule(),
  });
}
