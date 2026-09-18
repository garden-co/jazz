import { col as s } from "../dsl.js";
import { defineTable } from "../table-definition.js";
import { rel, reverse } from "../relationships.js";

export const deviceRequestSchema = {
  __e2ee_recovery_roots: defineTable(
    {
      accountId: s.uuid(),
      signerId: s.uuid(),
      epochId: s.string(),
      signingPublicKey: s.bytes(),
      signingMechanism: s.string(),
      signingVersion: s.int(),
      publicKey: s.bytes(),
      mechanism: s.string(),
      version: s.int(),
      signature: s.bytes(),
    },
    {
      account: rel("__e2ee_account_identities", "accountId"),
      signer: rel("__e2ee_device_requests", "signerId"),
    },
  ),
  __e2ee_public_account_successors: defineTable(
    {
      accountId: s.uuid(),
      predecessor: s.string(),
      epochId: s.string(),
      signerId: s.uuid(),
      removedDeviceId: s.uuid(),
      membership: s.bytes(),
      revision: s.bytes(),
      signature: s.bytes(),
    },
    {
      account: rel("__e2ee_account_identities", "accountId"),
      signer: rel("__e2ee_device_requests", "signerId"),
      removedDevice: rel("__e2ee_device_requests", "removedDeviceId"),
    },
  ),
  __e2ee_public_device_approvals: defineTable(
    {
      recoveryRootId: s.uuid().optional(),
      recoverySignature: s.bytes().optional(),
      accountId: s.uuid(),
      epochId: s.string(),
      deviceId: s.uuid(),
      signerId: s.uuid(),
      signature: s.bytes(),
    },
    {
      recoveryRoot: rel("__e2ee_recovery_roots", "recoveryRootId"),
      account: rel("__e2ee_account_identities", "accountId"),
      device: rel("__e2ee_device_requests", "deviceId"),
      signer: rel("__e2ee_device_requests", "signerId"),
    },
  ),
  __e2ee_account_roots: defineTable(
    {
      ledgerVersion: s.int().default(0),
      accountId: s.uuid(),
      deviceId: s.uuid(),
      epochId: s.string(),
    },
    {
      account: rel("__e2ee_account_identities", "accountId"),
      device: rel("__e2ee_device_requests", "deviceId"),
    },
  ),
  __e2ee_device_keys: defineTable(
    {
      deviceId: s.uuid(),
      signingPublicKey: s.bytes(),
      signingMechanism: s.string(),
      signingVersion: s.int(),
      publicKey: s.bytes(),
      mechanism: s.string(),
      version: s.int(),
    },
    {
      device: rel("__e2ee_device_requests", "deviceId"),
    },
  ),
  __e2ee_account_successors: defineTable(
    {
      accountId: s.uuid(),
      predecessor: s.string(),
      epochId: s.string(),
      signerId: s.uuid(),
      removedDeviceId: s.uuid(),
      membership: s.bytes(),
      revision: s.bytes(),
      verification: s.bytes(),
      history: s.bytes(),
      deliveries: s.bytes(),
      signature: s.bytes(),
    },
    {
      account: rel("__e2ee_account_identities", "accountId"),
      signer: rel("__e2ee_device_requests", "signerId"),
      removedDevice: rel("__e2ee_device_requests", "removedDeviceId"),
    },
  ),
  __e2ee_device_challenges: defineTable(
    {
      deviceId: s.uuid(),
      epochId: s.string(),
      envelope: s.bytes(),
    },
    {
      device: rel("__e2ee_device_requests", "deviceId"),
    },
  ),
  __e2ee_device_proofs: defineTable(
    {
      signature: s.bytes(),
      challengeId: s.uuid(),
      proof: s.bytes(),
    },
    {
      challenge: rel("__e2ee_device_challenges", "challengeId"),
    },
  ),
  __e2ee_device_approvals: defineTable(
    {
      signerId: s.uuid(),
      signature: s.bytes(),
      challengeId: s.uuid(),
      verification: s.bytes(),
    },
    {
      signer: rel("__e2ee_device_requests", "signerId"),
      challenge: rel("__e2ee_device_challenges", "challengeId"),
    },
  ),
  __e2ee_device_deliveries: defineTable(
    {
      challengeId: s.uuid(),
      envelope: s.bytes(),
      verification: s.bytes(),
    },
    {
      challenge: rel("__e2ee_device_challenges", "challengeId"),
    },
  ),
  __e2ee_account_identities: defineTable(
    {
      ledgerVersion: s.int().default(0),
      deviceId: s.uuid(),
      epochId: s.string(),
      envelope: s.bytes(),
      verification: s.bytes(),
    },
    {
      device: rel("__e2ee_device_requests", "deviceId"),
      __e2ee_public_account_successorsViaAccount: reverse(
        "__e2ee_public_account_successors",
        "account",
      ),
      __e2ee_public_device_approvalsViaAccount: reverse(
        "__e2ee_public_device_approvals",
        "account",
      ),
    },
  ),
  __e2ee_device_requests: defineTable(
    {
      signingPublicKey: s.bytes(),
      signingMechanism: s.string(),
      signingVersion: s.int(),
      publicKey: s.bytes(),
      mechanism: s.string(),
      version: s.int(),
      challenge: s.bytes(),
    },
    {},
  ),
};
