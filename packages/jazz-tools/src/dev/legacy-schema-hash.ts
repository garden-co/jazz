// Schema hashes as Jazz computed them before alpha.60: BLAKE3 over the same
// structural bytes that are now hashed with SHA-256. Only `validate` uses
// this, to recognise migrations and snapshots named by those hashes; nothing
// at runtime does.
import { blake3 } from "@noble/hashes/blake3.js";
import { bytesToHex } from "@noble/hashes/utils.js";
import type { WasmSchema } from "../drivers/types.js";
import { structuralSchemaBytes } from "./schema-utils.js";

/** The schema hash an alpha.59 or older tool gave `schema`. */
export function legacySchemaHash(schema: WasmSchema): string {
  return bytesToHex(blake3(structuralSchemaBytes(schema)));
}
