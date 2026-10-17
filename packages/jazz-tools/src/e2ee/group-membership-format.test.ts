import { expect, it } from "vitest";
import { groupMembershipBytes } from "./group-format.js";
import { createNativeDeviceSigner } from "./native.js";

const record = {
  id: "11111111-1111-4111-8111-111111111111",
  groupId: "22222222-2222-4222-8222-222222222222",
  epochId: "33333333-3333-4333-8333-333333333333",
  authorAccountId: "author-account",
  authorDeviceId: "44444444-4444-4444-8444-444444444444",
  authorEpochId: "55555555-5555-4555-8555-555555555555",
  operation: "add",
  memberKind: "account",
  memberId: "recipient-account",
};

it("binds every group membership coordinate and operation to its device signature", async () => {
  const signer = await createNativeDeviceSigner();
  const device = await signer.createKeyPair();
  try {
    const bytes = groupMembershipBytes("fixture", record);
    // Literal JE2C v1 fixture, framed independently of the production encoder.
    expect(Buffer.from(bytes).toString("hex")).toBe(
      "4a4532430100000007666978747572650000001d6a617a7a2e653265652e67726f75702d6d656d626572736869702e76310000000567726f75700000002432323232323232322d323232322d343232322d383232322d323232323232323232323232000000175f5f653265655f67726f75705f6d656d626572736869700000002431313131313131312d313131312d343131312d383131312d313131313131313131313131000000665b22616464222c22617574686f722d6163636f756e74222c2234343434343434342d343434342d343434342d383434342d343434343434343434343434222c2235353535353535352d353535352d343535352d383535352d353535353535353535353535225d0000002433333333333333332d333333332d343333332d383333332d3333333333333333333333330000001f5b226163636f756e74222c22726563697069656e742d6163636f756e74225d",
    );
    const signature = await signer.sign(device.privateKey, bytes);
    expect(await signer.verify(device.publicKey, bytes, signature)).toBe(true);
    const otherId = "66666666-6666-4666-8666-666666666666";
    for (const changed of [
      { id: otherId },
      { groupId: otherId },
      { epochId: otherId },
      { authorAccountId: "other-account" },
      { authorDeviceId: otherId },
      { authorEpochId: otherId },
      { operation: "remove" },
      { memberKind: "group", memberId: otherId },
      { memberId: "other-recipient" },
    ])
      expect(
        await signer.verify(
          device.publicKey,
          groupMembershipBytes("fixture", { ...record, ...changed }),
          signature,
        ),
      ).toBe(false);
    expect(
      await signer.verify(device.publicKey, groupMembershipBytes("other-app", record), signature),
    ).toBe(false);
    expect(() => groupMembershipBytes("fixture", { ...record, operation: "replace" })).toThrow();
    expect(() => groupMembershipBytes("fixture", { ...record, memberKind: "device" })).toThrow();
  } finally {
    device.privateKey.fill(0);
  }
});
