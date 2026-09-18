import { expect, it } from "vitest";
import { createBrowserDeviceSigner } from "./browser.js";
import { spaceSuccessorBytes } from "./space-successor.js";

const root = {
  id: "11111111-1111-4111-8111-111111111111",
  scopeId: "77777777-7777-4777-8777-777777777777",
  identifier: "88888888-8888-4888-8888-888888888888",
};
const encoder = new TextEncoder();
const record = {
  id: "66666666-6666-4666-8666-666666666666",
  spaceId: root.id,
  predecessor: "33333333-3333-4333-8333-333333333333",
  epochId: "22222222-2222-4222-8222-222222222222",
  authorAccountId: "account-id",
  authorDeviceId: "44444444-4444-4444-8444-444444444444",
  authorEpochId: "55555555-5555-4555-8555-555555555555",
  revision: encoder.encode("[]"),
  membership: encoder.encode('[["account-id","55555555-5555-4555-8555-555555555555"]]'),
  verification: new Uint8Array([0xaa]),
  history: new Uint8Array([0xbb]),
  authorEnvelope: new Uint8Array([0xcc]),
};
const other = "99999999-9999-4999-8999-999999999999";

it("matches the independent C space-successor transcript", () => {
  // SPEC/fixtures/e2ee-space-successor.c emits this literal independently.
  const expected =
    "AAABe0pFMkMBAAAAB2ZpeHR1cmUAAAASamF6ei5lMmVlLnNwYWNlLnYxAAAAJDc3Nzc3Nzc3LTc3NzctNDc3Ny04Nzc3LTc3Nzc3Nzc3Nzc3NwAAACQ4ODg4ODg4OC04ODg4LTQ4ODgtODg4OC04ODg4ODg4ODg4ODgAAAANX19lMmVlX3NwYWNlcwAAACQxMTExMTExMS0xMTExLTQxMTEtODExMS0xMTExMTExMTExMTEAAABAWyJzdWNjZXNzb3IiLCI2NjY2NjY2Ni02NjY2LTQ2NjYtODY2Ni02NjY2NjY2NjY2NjYiLCJzaWduYXR1cmUiXQAAACQyMjIyMjIyMi0yMjIyLTQyMjItODIyMi0yMjIyMjIyMjIyMjIAAABcWyJhY2NvdW50LWlkIiwiNDQ0NDQ0NDQtNDQ0NC00NDQ0LTg0NDQtNDQ0NDQ0NDQ0NDQ0IiwiNTU1NTU1NTUtNTU1NS00NTU1LTg1NTUtNTU1NTU1NTU1NTU1Il0AAAAkMzMzMzMzMzMtMzMzMy00MzMzLTgzMzMtMzMzMzMzMzMzMzMzAAAAAltdAAAAN1tbImFjY291bnQtaWQiLCI1NTU1NTU1NS01NTU1LTQ1NTUtODU1NS01NTU1NTU1NTU1NTUiXV0AAAABqgAAAAG7AAAAAcw=";
  expect(spaceSuccessorBytes("fixture", root, record)).toEqual(
    new Uint8Array(Buffer.from(expected, "base64")),
  );
});

it("binds space coordinates, predecessor, membership and encrypted history to the device signature", async () => {
  const signer = await createBrowserDeviceSigner();
  const device = await signer.createKeyPair();
  try {
    const bytes = spaceSuccessorBytes("fixture", root, record);
    const signature = await signer.sign(device.privateKey, bytes);
    expect(await signer.verify(device.publicKey, bytes, signature)).toBe(true);
    for (const changes of [
      { id: other },
      { predecessor: other },
      { epochId: other },
      { authorAccountId: "other-account" },
      { authorDeviceId: other },
      { authorEpochId: other },
      { revision: encoder.encode('["different-grant"]') },
      { membership: encoder.encode('[["other-account","55555555-5555-4555-8555-555555555555"]]') },
      { verification: new Uint8Array([1]) },
      { history: new Uint8Array([1]) },
      { authorEnvelope: new Uint8Array([1]) },
    ]) {
      expect(
        await signer.verify(
          device.publicKey,
          spaceSuccessorBytes("fixture", root, { ...record, ...changes }),
          signature,
        ),
      ).toBe(false);
    }
    for (const changes of [{ scopeId: other }, { identifier: other }])
      expect(
        await signer.verify(
          device.publicKey,
          spaceSuccessorBytes("fixture", { ...root, ...changes }, record),
          signature,
        ),
      ).toBe(false);
    expect(
      await signer.verify(
        device.publicKey,
        spaceSuccessorBytes("other-app", root, record),
        signature,
      ),
    ).toBe(false);
    expect(
      await signer.verify(
        device.publicKey,
        spaceSuccessorBytes("fixture", { ...root, id: other }, { ...record, spaceId: other }),
        signature,
      ),
    ).toBe(false);
    expect(() => spaceSuccessorBytes("fixture", root, { ...record, spaceId: other })).toThrow();
    expect(() =>
      spaceSuccessorBytes("fixture", root, { ...record, predecessor: record.epochId }),
    ).toThrow();
    expect(() =>
      spaceSuccessorBytes("fixture", root, { ...record, revision: encoder.encode(" []") }),
    ).toThrow();
    expect(() =>
      spaceSuccessorBytes("fixture", root, { ...record, membership: encoder.encode(" []") }),
    ).toThrow();
  } finally {
    device.privateKey.fill(0);
  }
});
