import { encodeCryptoContext } from "./context.js";
import { encodeEnvelope } from "./envelope.js";
import { frameCryptoRecord } from "./record-frame.js";
import { runtimeRandomBytes } from "../runtime/runtime-entropy.js";
import type { DeviceKeyPair, DeviceSigner, KeyEnvelope } from "./types.js";
import { E2eeRecoveryError } from "./recovery-error.js";

/** Owned private buffers: the recovery operation must clear them in finally. */
export async function decodeRecoveryMaterial(
  value: string,
  expectedScope: string,
  keys: KeyEnvelope,
  signer: DeviceSigner,
): Promise<{ rootId: string; recipient: DeviceKeyPair; signing: DeviceKeyPair }> {
  let privateFields: Record<string, unknown> | null | undefined;
  let material: { rootId: string; recipient: DeviceKeyPair; signing: DeviceKeyPair } | undefined;
  try {
    if (typeof value !== "string" || value.length > 2_000_000)
      throw new Error("Invalid E2EE recovery material size");
    const parsed = JSON.parse(value) as Record<string, unknown> | null;
    privateFields = parsed;
    const fields = [
      "format",
      "scope",
      "rootId",
      "mechanism",
      "version",
      "publicKey",
      "privateKey",
      "signingMechanism",
      "signingVersion",
      "signingPublicKey",
      "signingPrivateKey",
    ];
    if (
      !parsed ||
      Array.isArray(parsed) ||
      typeof parsed !== "object" ||
      parsed.format !== "jazz-e2ee-recovery-v1" ||
      Object.keys(parsed).length !== fields.length ||
      fields.some((field) => !Object.hasOwn(parsed, field))
    )
      throw new Error("Invalid E2EE recovery material format");
    if (parsed.scope !== expectedScope)
      throw new Error("E2EE recovery material belongs to another account scope");
    if (
      typeof parsed.rootId !== "string" ||
      !/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/.test(parsed.rootId)
    )
      throw new Error("Invalid E2EE recovery root ID");
    if (
      parsed.mechanism !== keys.mechanism.id ||
      parsed.version !== keys.mechanism.version ||
      parsed.signingMechanism !== signer.mechanism.id ||
      parsed.signingVersion !== signer.mechanism.version
    )
      throw new Error("E2EE recovery material requires its original mechanisms");
    const bytes = (value: unknown): number[] => {
      if (
        !Array.isArray(value) ||
        value.length === 0 ||
        value.length > 65536 ||
        !value.every((byte) => Number.isInteger(byte) && byte >= 0 && byte <= 255)
      )
        throw new Error("Invalid E2EE recovery key bytes");
      return value;
    };
    // Validate all arrays before allocating private buffers, so a later malformed
    // field cannot strand an earlier secret copy outside the cleanup scope.
    const publicKey = bytes(parsed.publicKey);
    const privateKey = bytes(parsed.privateKey);
    const signingPublicKey = bytes(parsed.signingPublicKey);
    const signingPrivateKey = bytes(parsed.signingPrivateKey);
    material = {
      rootId: parsed.rootId,
      recipient: { publicKey: Uint8Array.from(publicKey), privateKey: Uint8Array.from(privateKey) },
      signing: {
        publicKey: Uint8Array.from(signingPublicKey),
        privateKey: Uint8Array.from(signingPrivateKey),
      },
    };
    const context = new TextEncoder().encode(
      `jazz.e2ee.recovery-material-check.v1\0${expectedScope}\0${material.rootId}`,
    );
    const challenge = runtimeRandomBytes(32);
    try {
      const envelope = await keys.seal(material.recipient.publicKey, context, challenge);
      const opened = await keys.open(material.recipient, context, envelope);
      try {
        if (opened.length !== challenge.length || !opened.every((byte, i) => byte === challenge[i]))
          throw new Error("Invalid E2EE recovery recipient keypair");
      } finally {
        opened.fill(0);
      }
      const signature = await signer.sign(material.signing.privateKey, context);
      if (!(await signer.verify(material.signing.publicKey, context, signature)))
        throw new Error("Invalid E2EE recovery signing keypair");
    } finally {
      challenge.fill(0);
    }
    return material;
  } catch {
    material?.recipient.privateKey.fill(0);
    material?.signing.privateKey.fill(0);
    throw new E2eeRecoveryError("recovery-material-unusable");
  } finally {
    if (Array.isArray(privateFields?.privateKey)) privateFields.privateKey.fill(0);
    if (Array.isArray(privateFields?.signingPrivateKey)) privateFields.signingPrivateKey.fill(0);
  }
}

/** Key-free recovery authority, independently generated from every device key. */
export type RecoveryRoot = {
  id: string;
  accountId: string;
  epochId: string;
  signerId: string;
  signingMechanism: string;
  signingVersion: number;
  signingPublicKey: Uint8Array;
  mechanism: string;
  version: number;
  publicKey: Uint8Array;
  signature: Uint8Array;
};

/** Client-only secret material. Never persist this string in replicated rows. */
export function encodeRecoveryMaterial(
  scope: string,
  root: Omit<RecoveryRoot, "signature">,
  privateKey: Uint8Array,
  signingPrivateKey: Uint8Array,
): string {
  for (const key of [root.publicKey, privateKey, root.signingPublicKey, signingPrivateKey]) {
    if (key.length === 0 || key.length > 65536) throw new Error("Invalid E2EE recovery key length");
  }
  const secret = Array.from(privateKey);
  const signingSecret = Array.from(signingPrivateKey);
  try {
    return JSON.stringify({
      format: "jazz-e2ee-recovery-v1",
      scope,
      rootId: root.id,
      mechanism: root.mechanism,
      version: root.version,
      publicKey: Array.from(root.publicKey),
      privateKey: secret,
      signingMechanism: root.signingMechanism,
      signingVersion: root.signingVersion,
      signingPublicKey: Array.from(root.signingPublicKey),
      signingPrivateKey: signingSecret,
    });
  } finally {
    secret.fill(0);
    signingSecret.fill(0);
  }
}

export function recoveryDeliveryContext(
  application: string,
  accountId: string,
  delivery: { id: string; rootId: string; epochId: string },
): Uint8Array {
  return encodeCryptoContext({
    application,
    policy: "jazz.e2ee.recovery.v1",
    scope: "account",
    identifier: accountId,
    table: "__e2ee_recovery_deliveries",
    row: delivery.id,
    column: "envelope",
    epoch: delivery.epochId,
    recipient: delivery.rootId,
  });
}

/** Signing bytes only; authority acceptance and key validation belong to replay. */
export function recoveryRootBytes(
  application: string,
  root: Omit<RecoveryRoot, "signature">,
): Uint8Array {
  return frameCryptoRecord([
    encodeCryptoContext({
      application,
      policy: "jazz.e2ee.recovery.v1",
      scope: "account",
      identifier: root.accountId,
      table: "__e2ee_recovery_roots",
      row: root.id,
      column: JSON.stringify(["root", root.signerId]),
      epoch: root.epochId,
      recipient: "",
    }),
    encodeEnvelope(
      { id: root.signingMechanism, version: root.signingVersion },
      root.signingPublicKey,
    ),
    encodeEnvelope({ id: root.mechanism, version: root.version }, root.publicKey),
  ]);
}
