import { col as s } from "../dsl.js";
import { defineTable } from "../table-definition.js";
import { rel, reverse } from "../relationships.js";

export const deviceRequestSchema = {
  __e2ee_recovery_protectors: defineTable(
    {
      rootId: s.uuid(),
      material: s.bytes(),
    },
    {
      root: rel("__e2ee_recovery_roots", "rootId"),
    },
  ),
  __e2ee_recovery_deliveries: defineTable(
    {
      rootId: s.uuid(),
      epochId: s.string(),
      envelope: s.bytes(),
    },
    {
      root: rel("__e2ee_recovery_roots", "rootId"),
    },
  ),
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

export const groupSchema = {
  __e2ee_group_recovery_deliveries: defineTable(
    {
      groupId: s.uuid(),
      epochId: s.string(),
      senderAccountId: s.uuid(),
      senderDeviceId: s.uuid(),
      recipientAccountId: s.uuid(),
      recoveryRootId: s.uuid(),
      envelope: s.bytes(),
      signature: s.bytes(),
    },
    {
      group: rel("__e2ee_groups", "groupId"),
      senderAccount: rel("__e2ee_account_identities", "senderAccountId"),
      senderDevice: rel("__e2ee_device_requests", "senderDeviceId"),
      recipientAccount: rel("__e2ee_account_identities", "recipientAccountId"),
      recoveryRoot: rel("__e2ee_recovery_roots", "recoveryRootId"),
    },
  ),
  __e2ee_group_successors: defineTable(
    {
      groupId: s.uuid(),
      predecessor: s.string(),
      epochId: s.string(),
      authorAccountId: s.uuid(),
      authorDeviceId: s.uuid(),
      authorEpochId: s.string(),
      revision: s.bytes(),
      membership: s.bytes(),
      verification: s.bytes(),
      history: s.bytes(),
      authorEnvelope: s.bytes(),
      signature: s.bytes(),
    },
    {
      group: rel("__e2ee_groups", "groupId"),
      authorAccount: rel("__e2ee_account_identities", "authorAccountId"),
      authorDevice: rel("__e2ee_device_requests", "authorDeviceId"),
    },
  ),
  __e2ee_group_repairs: defineTable(
    {
      groupId: s.uuid(),
      epochId: s.string(),
      deliveryId: s.uuid(),
      accountId: s.uuid(),
      deviceId: s.uuid(),
      accountEpochId: s.string(),
      signature: s.bytes(),
    },
    {
      group: rel("__e2ee_groups", "groupId"),
      delivery: rel("__e2ee_group_deliveries", "deliveryId"),
      account: rel("__e2ee_account_identities", "accountId"),
      device: rel("__e2ee_device_requests", "deviceId"),
    },
  ),
  __e2ee_group_membership: defineTable(
    {
      groupId: s.uuid(),
      epochId: s.string(),
      authorAccountId: s.uuid(),
      authorDeviceId: s.string(),
      authorEpochId: s.string(),
      operation: s.string(),
      memberKind: s.string(),
      memberId: s.uuid(),
      signature: s.bytes(),
    },
    {
      group: rel("__e2ee_groups", "groupId"),
      authorAccount: rel("__e2ee_account_identities", "authorAccountId"),
    },
  ),
  __e2ee_groups: defineTable(
    {
      accountId: s.uuid(),
      deviceId: s.uuid(),
      accountEpochId: s.string(),
      epochId: s.string(),
      mechanism: s.string(),
      version: s.int(),
      verification: s.bytes(),
      signature: s.bytes(),
    },
    {
      account: rel("__e2ee_account_identities", "accountId"),
      device: rel("__e2ee_device_requests", "deviceId"),
      __e2ee_group_membershipViaGroup: reverse("__e2ee_group_membership", "group"),
      __e2ee_group_successorsViaGroup: reverse("__e2ee_group_successors", "group"),
      __e2ee_group_deliveriesViaGroup: reverse("__e2ee_group_deliveries", "group"),
      __e2ee_group_repairsViaGroup: reverse("__e2ee_group_repairs", "group"),
    },
  ),
  __e2ee_group_deliveries: defineTable(
    {
      groupId: s.uuid(),
      epochId: s.string(),
      senderAccountId: s.uuid(),
      senderDeviceId: s.uuid(),
      recipientAccountId: s.uuid(),
      recipientDeviceId: s.uuid(),
      envelope: s.bytes(),
      signature: s.bytes(),
    },
    {
      group: rel("__e2ee_groups", "groupId"),
      senderAccount: rel("__e2ee_account_identities", "senderAccountId"),
      senderDevice: rel("__e2ee_device_requests", "senderDeviceId"),
      recipientAccount: rel("__e2ee_account_identities", "recipientAccountId"),
      recipientDevice: rel("__e2ee_device_requests", "recipientDeviceId"),
    },
  ),
};

export const spaceSchema = {
  __e2ee_space_recovery_deliveries: defineTable(
    {
      spaceId: s.uuid(),
      epochId: s.uuid(),
      senderAccountId: s.uuid(),
      senderDeviceId: s.uuid(),
      senderEpochId: s.uuid(),
      recipientAccountId: s.uuid(),
      recipientEpochId: s.uuid(),
      recoveryRootId: s.uuid(),
      envelope: s.bytes(),
      signature: s.bytes(),
    },
    {
      space: rel("__e2ee_spaces", "spaceId"),
      senderAccount: rel("__e2ee_account_identities", "senderAccountId"),
      senderDevice: rel("__e2ee_device_requests", "senderDeviceId"),
      recipientAccount: rel("__e2ee_account_identities", "recipientAccountId"),
      recoveryRoot: rel("__e2ee_recovery_roots", "recoveryRootId"),
    },
  ),
  __e2ee_space_successors: defineTable(
    {
      spaceId: s.uuid(),
      predecessor: s.uuid(),
      epochId: s.uuid(),
      authorAccountId: s.uuid(),
      authorDeviceId: s.uuid(),
      authorEpochId: s.uuid(),
      revision: s.bytes(),
      membership: s.bytes(),
      verification: s.bytes(),
      history: s.bytes(),
      authorEnvelope: s.bytes(),
      signature: s.bytes(),
    },
    {
      space: rel("__e2ee_spaces", "spaceId"),
      authorAccount: rel("__e2ee_account_identities", "authorAccountId"),
      authorDevice: rel("__e2ee_device_requests", "authorDeviceId"),
    },
  ),
  __e2ee_spaces: defineTable(
    {
      scopeId: s.uuid(),
      identifier: s.uuid(),
      accountId: s.uuid(),
      deviceId: s.uuid(),
      accountEpochId: s.uuid(),
      epochId: s.uuid(),
      initialGrantId: s.uuid(),
      mechanism: s.string(),
      version: s.int(),
      verification: s.bytes(),
      authorEnvelope: s.bytes(),
      signature: s.bytes(),
    },
    {
      account: rel("__e2ee_account_identities", "accountId"),
      device: rel("__e2ee_device_requests", "deviceId"),
      __e2ee_space_successorsViaSpace: reverse("__e2ee_space_successors", "space"),
      __e2ee_space_grantsViaSpace: reverse("__e2ee_space_grants", "space"),
      __e2ee_space_deliveriesViaSpace: reverse("__e2ee_space_deliveries", "space"),
    },
  ),
  __e2ee_space_grants: defineTable(
    {
      spaceId: s.uuid(),
      epochId: s.uuid(),
      authorAccountId: s.uuid(),
      authorDeviceId: s.uuid(),
      authorEpochId: s.uuid(),
      operation: s.string(),
      recipientKind: s.string(),
      recipientId: s.uuid(),
      recipientEpochId: s.uuid(),
      signature: s.bytes(),
    },
    {
      space: rel("__e2ee_spaces", "spaceId"),
      authorAccount: rel("__e2ee_account_identities", "authorAccountId"),
      authorDevice: rel("__e2ee_device_requests", "authorDeviceId"),
    },
  ),
  __e2ee_space_deliveries: defineTable(
    {
      spaceId: s.uuid(),
      epochId: s.uuid(),
      senderAccountId: s.uuid(),
      senderDeviceId: s.uuid(),
      senderEpochId: s.uuid(),
      recipientAccountId: s.uuid(),
      recipientDeviceId: s.uuid(),
      recipientEpochId: s.uuid(),
      envelope: s.bytes(),
      signature: s.bytes(),
    },
    {
      space: rel("__e2ee_spaces", "spaceId"),
      senderAccount: rel("__e2ee_account_identities", "senderAccountId"),
      senderDevice: rel("__e2ee_device_requests", "senderDeviceId"),
      recipientAccount: rel("__e2ee_account_identities", "recipientAccountId"),
      recipientDevice: rel("__e2ee_device_requests", "recipientDeviceId"),
    },
  ),
};
