import { createDbWithRuntimeSource, Db } from "../runtime/db.js";
import { ReactNativeRuntimeSource, type ReactNativeDbConfig } from "./runtime-source.js";

export { Db };
export type DbConfig = ReactNativeDbConfig;

export async function createDb(config: ReactNativeDbConfig): Promise<Db> {
  return await createDbWithRuntimeSource(config, new ReactNativeRuntimeSource());
}
