import jazzRn from "jazz-rn";
import type { WasmSchema } from "../drivers/types.js";
import type { DurabilityTier } from "../runtime/client.js";
import { JazzRnRuntimeAdapter } from "./jazz-rn-runtime-adapter.js";

export interface CreateJazzRnRuntimeOptions {
  schema: WasmSchema;
  appId: string;
  env?: string;
  userBranch?: string;
  tier?: DurabilityTier;
  dataPath?: string;
}

export function createJazzRnRuntime(options: CreateJazzRnRuntimeOptions): JazzRnRuntimeAdapter {
  const runtime = new jazzRn.jazz_rn.RnRuntime(
    JSON.stringify(options.schema),
    options.appId,
    options.env ?? "dev",
    options.userBranch ?? "main",
    options.tier,
    options.dataPath,
  );

  return new JazzRnRuntimeAdapter(runtime, options.schema);
}
