import { DefaultRuntimeSource } from "./default-runtime-source.js";
import { createDbWithRuntimeSource, type Db, type DbConfig } from "./db.js";

/** Create a browser/Node database backed by the WASM runtime. */
export async function createDb(config: DbConfig): Promise<Db> {
  return await createDbWithRuntimeSource(config, new DefaultRuntimeSource());
}
