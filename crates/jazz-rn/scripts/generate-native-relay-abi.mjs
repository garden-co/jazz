#!/usr/bin/env node

/** Generate the TypeScript mirror of the Rust-owned native relay ABI. */
import { readFileSync, writeFileSync } from "node:fs";
import { resolve } from "node:path";

const root = resolve(import.meta.dirname, "../../..");
const rustSource = readFileSync(resolve(root, "crates/jazz-native-relay/src/lib.rs"), "utf8");
const protocolAbi = /^pub const NATIVE_RELAY_ABI_V1: u16 = (\d+);$/m.exec(rustSource)?.[1];
const artifactAbi = /^pub const NATIVE_RELAY_ABI_V2: u16 = (\d+);$/m.exec(rustSource)?.[1];
if (!protocolAbi || !artifactAbi)
  throw new Error(
    "could not read native relay protocol and artifact ABI versions from Rust source",
  );

const target = resolve(root, "crates/jazz-rn/src/native-relay-abi.ts");
const rendered = `/**\n * Generated from \`crates/jazz-native-relay/src/lib.rs\` by\n * \`scripts/generate-native-relay-abi.mjs\`. Do not edit this value by hand.\n *\n * Rust owns the protocol and artifact ABIs. Native hosts ask their linked artifact\n * version at runtime through \`jazz_native_relay_abi_version()\`, while TypeScript\n * imports this checked-in generated mirror before it sends any bytes.\n */\nexport const NATIVE_RELAY_ABI_V1 = ${protocolAbi} as const;\nexport const NATIVE_RELAY_ABI_V2 = ${artifactAbi} as const;\n\nexport const NATIVE_RELAY_ABI = {\n  minimum: NATIVE_RELAY_ABI_V2,\n  maximum: NATIVE_RELAY_ABI_V2,\n} as const;\n`;

if (process.argv.includes("--check")) {
  if (readFileSync(target, "utf8") !== rendered)
    throw new Error(
      "native-relay-abi.ts is stale; run node crates/jazz-rn/scripts/generate-native-relay-abi.mjs",
    );
} else {
  writeFileSync(target, rendered);
}
