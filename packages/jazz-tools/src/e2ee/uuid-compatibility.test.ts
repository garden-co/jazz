import { readFileSync } from "node:fs";
import { expect, it } from "vitest";
import { successorSigningBytes, publicSuccessorSigningBytes } from "./account-successor.js";
import { publicDeviceApprovalBytes } from "./public-device-approval.js";
import { groupRootBytes, groupMembershipBytes, groupRepairBytes } from "./group-format.js";
import { groupSuccessorSigningBytes } from "./group-successor.js";
import { groupRecoveryBytes } from "./group-recovery-format.js";
import { DeviceKeyLifetime, localDevice } from "./local-device.js";
import { createNativeCrypto } from "./native.js";

const v4 = "11111111-1111-4111-8111-111111111111";
const v7 = "11111111-1111-7111-8111-111111111111";
const predecessor = "22222222-2222-7222-8222-222222222222";
const removed = "33333333-3333-4333-8333-333333333333";
const json = (value: unknown) => new TextEncoder().encode(JSON.stringify(value));

// Mix v4 and v7 coordinates, including IDs embedded in canonical membership JSON.
function transcripts(id: string): Record<string, () => Uint8Array> {
  const account = {
    id,
    accountId: "account",
    epochId: id,
    predecessor,
    signerId: id,
    action: "remove-device" as const,
    removedDeviceId: removed,
    membership: json([id]),
    revision: json([id]),
    deliveries: json([[id, [1]]]),
    verification: Uint8Array.of(1),
    history: Uint8Array.of(2),
  };
  return {
    "public approval": () => publicDeviceApprovalBytes("fixture", { ...account, deviceId: id }),
    "private successor": () => successorSigningBytes("fixture", account),
    "public successor": () => publicSuccessorSigningBytes("fixture", account),
    "recovery retirement": () =>
      successorSigningBytes("fixture", {
        ...account,
        action: "retire-recovery-root",
        removedDeviceId: undefined,
        retiredRecoveryRootId: id,
      }),
    "group root": () =>
      groupRootBytes("fixture", {
        id,
        accountId: "account",
        epochId: id,
        deviceId: id,
        accountEpochId: id,
        mechanism: "test",
        version: 1,
        verification: Uint8Array.of(1),
      }),
    "group membership": () =>
      groupMembershipBytes("fixture", {
        id,
        groupId: id,
        epochId: id,
        authorAccountId: "account",
        authorDeviceId: id,
        authorEpochId: id,
        operation: "add",
        memberKind: "account",
        memberId: "recipient",
      }),
    "group repair": () =>
      groupRepairBytes("fixture", {
        id,
        groupId: id,
        epochId: id,
        deliveryId: id,
        accountId: "account",
        deviceId: id,
        accountEpochId: id,
      }),
    "group successor": () =>
      groupSuccessorSigningBytes("fixture", {
        id,
        groupId: id,
        epochId: id,
        predecessor,
        authorAccountId: "account",
        authorDeviceId: id,
        authorEpochId: id,
        revision: json([id]),
        membership: json([["account", id]]),
        verification: Uint8Array.of(1),
        history: Uint8Array.of(2),
        authorEnvelope: Uint8Array.of(3),
      }),
    "group recovery": () =>
      groupRecoveryBytes("fixture", {
        id,
        groupId: id,
        epochId: id,
        senderAccountId: "account",
        senderDeviceId: id,
        recipientAccountId: "recipient",
        recoveryRootId: id,
        envelope: Uint8Array.of(1),
      }),
  };
}

it("authenticates v4 and v7 IDs as distinct coordinates, while rejecting invalid UUIDs", async () => {
  const { deviceSigner: signer } = await createNativeCrypto();
  const key = await signer.createKeyPair();
  try {
    const four = transcripts(v4);
    const seven = transcripts(v7);
    for (const name of Object.keys(four)) {
      const a = four[name]!();
      const b = seven[name]!();
      const signedA = await signer.sign(key.privateKey, a);
      const signedB = await signer.sign(key.privateKey, b);
      expect(await signer.verify(key.publicKey, a, signedA), name).toBe(true);
      expect(await signer.verify(key.publicKey, b, signedB), name).toBe(true);
      expect(await signer.verify(key.publicKey, b, signedA), name).toBe(false);
      expect(await signer.verify(key.publicKey, a, signedB), name).toBe(false);
    }
    for (const invalid of [
      "11111111-1111-1111-8111-111111111111", // Unsupported version.
      "11111111-1111-7111-1111-111111111111", // Wrong variant.
      "11111111711181111111111111111111", // Missing hyphens.
    ])
      for (const [name, encode] of Object.entries(transcripts(invalid)))
        expect(encode, name).toThrow();
  } finally {
    key.privateKey.fill(0);
  }
});

it("loads and verifies a retained v7 device without rewriting its identity or keys", async () => {
  const fixture = readFileSync(new URL("./fixtures/local-device-v2.json", import.meta.url), "utf8");
  const value = fixture.replace(v4, v7);
  const scope = JSON.parse(value).devices[0].scope;
  const lifetime = new DeviceKeyLifetime();
  const crypto = await createNativeCrypto();
  try {
    const device = await localDevice(
      {
        async read() {
          return value;
        },
        async update() {
          throw new Error("Retained device must not be rewritten");
        },
      },
      scope,
      crypto.keyEnvelope,
      crypto.deviceSigner,
      lifetime,
      () => {},
    );
    expect(device?.id).toBe(v7);
    expect(device?.privateKey.some((byte) => byte !== 0)).toBe(true);
    lifetime.close();
    expect(device?.privateKey.every((byte) => byte === 0)).toBe(true);
    expect(device?.signing.privateKey.every((byte) => byte === 0)).toBe(true);
  } finally {
    lifetime.close();
  }
});
