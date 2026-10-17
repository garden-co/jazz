import { encodeCryptoContext } from "./context.js";
import type { CellCipher } from "./types.js";

type Target = { application: string; accountId: string; rootId: string };

/** Separate purpose and account binding; this is not a data-cell context. */
export function recoveryProtectionContext(target: Target): Uint8Array {
  if (!/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/.test(target.rootId))
    throw new Error("Invalid E2EE recovery root ID");
  return encodeCryptoContext({
    application: target.application,
    policy: "jazz.e2ee.local-recovery-protection.v1",
    scope: "account",
    identifier: target.accountId,
    table: "__e2ee_recovery_protectors",
    row: target.rootId,
    column: "material",
    epoch: target.rootId,
    recipient: target.rootId,
  });
}

/** The caller owns the random account secret; no password or device-key derivation. */
export async function protectRecoveryMaterial(
  cipher: CellCipher,
  secret: Uint8Array,
  target: Target,
  material: string,
): Promise<Uint8Array> {
  if (secret.length !== 32 || typeof material !== "string" || material.length > 2_000_000)
    throw new Error("Invalid E2EE recovery protection input");
  const context = recoveryProtectionContext(target);
  const bytes = new TextEncoder().encode(material);
  try {
    return await cipher.encrypt(secret, context, bytes);
  } finally {
    bytes.fill(0);
  }
}

/** Opening authenticates the wrapper, not the recovery root's historical authority. */
export async function openRecoveryMaterial(
  cipher: CellCipher,
  secret: Uint8Array,
  target: Target,
  envelope: Uint8Array,
): Promise<string> {
  if (secret.length !== 32) throw new Error("Invalid E2EE recovery protection secret");
  const bytes = await cipher.decrypt(secret, recoveryProtectionContext(target), envelope);
  try {
    if (bytes.length > 6_000_000) throw new Error("Invalid E2EE recovery material size");
    const material = new TextDecoder("utf-8", { fatal: true }).decode(bytes);
    if (material.length > 2_000_000) throw new Error("Invalid E2EE recovery material size");
    return material;
  } finally {
    bytes.fill(0);
  }
}
