import jazzRn from "jazz-rn";
import type { WasmSchema } from "../drivers/types.js";
import type { DurabilityTier } from "../runtime/client.js";
import { type JazzRnRuntimeBinding, JazzRnRuntimeAdapter } from "./jazz-rn-runtime-adapter.js";

export interface CreateJazzRnRuntimeOptions {
  schema: WasmSchema;
  appId: string;
  env?: string;
  userBranch?: string;
  tier?: DurabilityTier;
  dataPath?: string;
}

declare const process: { env: Record<string, string | undefined> };

let diagnosticLoggingInstalled = false;

function installDiagnosticLoggingOnce(): void {
  if (diagnosticLoggingInstalled) return;
  diagnosticLoggingInstalled = true;
  // Metro inlines EXPO_PUBLIC_* env vars into the bundle at build time.
  const filter = process.env.EXPO_PUBLIC_JAZZ_RN_TRACE;
  if (!filter) return;
  const init = (jazzRn.jazz_rn as { initDiagnosticLogging?: (filter: string) => void })
    .initDiagnosticLogging;
  if (typeof init === "function") {
    init(filter);
  }
}

export function createJazzRnRuntime(options: CreateJazzRnRuntimeOptions): JazzRnRuntimeAdapter {
  installDiagnosticLoggingOnce();
  const runtime = new jazzRn.jazz_rn.RnRuntime(
    JSON.stringify(options.schema),
    options.appId,
    options.env ?? "dev",
    options.userBranch ?? "main",
    options.tier,
    options.dataPath,
  );

  return new JazzRnRuntimeAdapter(runtime as unknown as JazzRnRuntimeBinding, options.schema);
}
