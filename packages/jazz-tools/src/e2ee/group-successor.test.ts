import { expect, it } from "vitest";
import {
  groupSuccessorSigningBytes,
  encodeGroupMembership,
  decodeGroupMembership,
} from "./group-successor.js";
import { createNativeDeviceSigner } from "./native.js";
import { createBrowserKeyEnvelope } from "./browser.js";
import { groupSuccessorContext } from "./group-successor.js";

it("authenticates the successor author's durable self-envelope", async () => {
  const id = "11111111-1111-4111-8111-111111111111";
  const keys = await createBrowserKeyEnvelope();
  const device = await keys.createKeyPair();
  const signer = await createNativeDeviceSigner();
  const signing = await signer.createKeyPair();
  const secret = new Uint8Array(32).fill(7);
  try {
    const coordinates = { id, groupId: id, epochId: id };
    const context = groupSuccessorContext("fixture", coordinates, "author-envelope", id);
    const authorEnvelope = await keys.seal(device.publicKey, context, secret);
    const record = {
      ...coordinates,
      predecessor: "22222222-2222-4222-8222-222222222222",
      authorAccountId: "account",
      authorDeviceId: id,
      authorEpochId: id,
      revision: new TextEncoder().encode("[]"),
      membership: encodeGroupMembership(new Map([["account", id]])),
      verification: new Uint8Array([1]),
      history: new Uint8Array([2]),
      authorEnvelope,
    };
    const bytes = groupSuccessorSigningBytes("fixture", record);
    const signature = await signer.sign(signing.privateKey, bytes);
    const altered = authorEnvelope.slice();
    altered[altered.length - 1] ^= 1;
    expect(
      await signer.verify(
        signing.publicKey,
        groupSuccessorSigningBytes("fixture", { ...record, authorEnvelope: altered }),
        signature,
      ),
    ).toBe(false);
    const opened = await keys.open(device, context, authorEnvelope);
    try {
      expect(opened).toEqual(secret);
    } finally {
      opened.fill(0);
    }
    await expect(
      keys.open(
        device,
        groupSuccessorContext("fixture", coordinates, "author-envelope", record.predecessor),
        authorEnvelope,
      ),
    ).rejects.toThrow();
  } finally {
    device.privateKey.fill(0);
    signing.privateKey.fill(0);
    secret.fill(0);
  }
});

it("binds a group successor to its predecessor, membership revision and signing device", async () => {
  const id = "11111111-1111-4111-8111-111111111111";
  const other = "22222222-2222-4222-8222-222222222222";
  const encoder = new TextEncoder();
  const record = {
    id,
    groupId: id,
    predecessor: other,
    epochId: id,
    authorAccountId: "account",
    authorDeviceId: id,
    authorEpochId: id,
    revision: encoder.encode('["candidate"]'),
    authorEnvelope: new Uint8Array([3]),
    membership: encodeGroupMembership(new Map([["account", id]])),
    verification: new Uint8Array([1]),
    history: new Uint8Array([2]),
  };
  expect(decodeGroupMembership(record.membership)).toEqual(new Map([["account", id]]));
  const bytes = groupSuccessorSigningBytes("fixture", record);
  // Literal transcript framed independently of the production encoders.
  expect(Buffer.from(bytes).toString("hex")).toBe(
    "000001364a4532430100000007666978747572650000001c6a617a7a2e653265652e67726f75702d737563636573736f722e76310000000567726f75700000002431313131313131312d313131312d343131312d383131312d313131313131313131313131000000175f5f653265655f67726f75705f737563636573736f72730000002431313131313131312d313131312d343131312d383131312d313131313131313131313131000000097369676e61747572650000002431313131313131312d313131312d343131312d383131312d313131313131313131313131000000595b226163636f756e74222c2231313131313131312d313131312d343131312d383131312d313131313131313131313131222c2231313131313131312d313131312d343131312d383131312d313131313131313131313131225d0000002432323232323232322d323232322d343232322d383232322d3232323232323232323232320000000d5b2263616e646964617465225d000000345b5b226163636f756e74222c2231313131313131312d313131312d343131312d383131312d313131313131313131313131225d5d000000010100000001020000000103",
  );
  const signer = await createNativeDeviceSigner();
  const key = await signer.createKeyPair();
  try {
    const signature = await signer.sign(key.privateKey, bytes);
    expect(await signer.verify(key.publicKey, bytes, signature)).toBe(true);
    for (const changed of [
      { id: other },
      { groupId: other },
      { predecessor: "33333333-3333-4333-8333-333333333333" },
      { epochId: other, predecessor: id },
      { authorAccountId: "another" },
      { authorDeviceId: other },
      { authorEpochId: other },
      { revision: encoder.encode('["different"]') },
      { membership: encodeGroupMembership(new Map([["account", other]])) },
      { verification: new Uint8Array([3]) },
      { history: new Uint8Array([4]) },
    ])
      expect(
        await signer.verify(
          key.publicKey,
          groupSuccessorSigningBytes("fixture", { ...record, ...changed }),
          signature,
        ),
      ).toBe(false);
    expect(
      await signer.verify(
        key.publicKey,
        groupSuccessorSigningBytes("other-app", record),
        signature,
      ),
    ).toBe(false);
    expect(() => groupSuccessorSigningBytes("fixture", { ...record, predecessor: id })).toThrow();
    expect(() =>
      groupSuccessorSigningBytes("fixture", { ...record, revision: encoder.encode('["x","x"]') }),
    ).toThrow();
    expect(() => decodeGroupMembership(encoder.encode('[["account","bad-epoch"]]'))).toThrow();
    expect(() =>
      decodeGroupMembership(encoder.encode('[["b","' + id + '"],["a","' + id + '"]]')),
    ).toThrow();
    expect(() =>
      decodeGroupMembership(encoder.encode('[["a","' + id + '"],["a","' + id + '"]]')),
    ).toThrow();
  } finally {
    key.privateKey.fill(0);
  }
});
