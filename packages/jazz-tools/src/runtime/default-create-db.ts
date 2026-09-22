import { DefaultRuntimeSource } from "./default-runtime-source.js";
import type { Db } from "./db.js";
import { createAccountDbWithRuntimeSource, type AccountDbConfig } from "../accounts/context.js";

/** Create a browser/Node database backed by the WASM runtime. */
export async function createDb(config: AccountDbConfig): Promise<Db> {
  return await createAccountDbWithRuntimeSource(config, new DefaultRuntimeSource());
}
