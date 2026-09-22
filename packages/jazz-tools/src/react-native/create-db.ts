import { Db } from "../runtime/db.js";
import { createAccountDbWithRuntimeSource, type AccountDbConfig } from "../accounts/context.js";
import { ReactNativeRuntimeSource, type ReactNativeDbConfig } from "./runtime-source.js";

export { Db };
export type DbConfig = AccountDbConfig & Pick<ReactNativeDbConfig, "nativeRelay" | "sqliteStorage">;

export async function createDb(config: DbConfig): Promise<Db> {
  return createAccountDbWithRuntimeSource(config, new ReactNativeRuntimeSource());
}
