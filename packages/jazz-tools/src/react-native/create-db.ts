import { createDbWithRuntimeSource, Db } from "../runtime/db.js";
import { ReactNativeRuntimeSource, type ReactNativeDbConfig } from "./runtime-source.js";

export { Db };
export type DbConfig = ReactNativeDbConfig;

export async function createDb(config: ReactNativeDbConfig): Promise<Db> {
  const source = new ReactNativeRuntimeSource();
  const admitted = { ...config };
  await source.load(admitted);
  source.admitConfig(admitted);
  return await createDbWithRuntimeSource(admitted, source);
}
