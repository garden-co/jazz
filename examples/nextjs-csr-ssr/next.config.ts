import { relative, resolve } from "node:path";
import type { NextConfig } from "next";

// Monorepo CI only: the correctness harness admits one sealed jazz-wasm
// build and names it in JAZZ_CORRECTNESS_WASM_PACKAGE. Every import of
// jazz-wasm must load that build, or the bundle carries two different copies.
function sealedWasmAlias(): Record<string, string> {
  const sealed = process.env.JAZZ_CORRECTNESS_WASM_PACKAGE;
  if (!sealed) return {};
  // Turbopack reads alias targets relative to the project.
  const entry = relative(process.cwd(), resolve(sealed, "jazz_wasm.js"));
  return { "jazz-wasm": entry.startsWith(".") ? entry : `./${entry}` };
}

export default {
  // jazz-napi is a native addon: Node loads it at runtime, the bundler leaves it alone.
  serverExternalPackages: ["jazz-napi"],
  turbopack: { resolveAlias: sealedWasmAlias() },
} satisfies NextConfig;
