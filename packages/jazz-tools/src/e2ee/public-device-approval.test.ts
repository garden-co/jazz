import { sign, createPrivateKey } from "node:crypto";
import { expect, it } from "vitest";
import { createBrowserDeviceSigner } from "./browser.js";
import { createNativeDeviceSigner } from "./native.js";
import { publicDeviceApprovalBytes } from "./public-device-approval.js";

const approval = {
  id: "11111111-1111-4111-8111-111111111111",
  accountId: "account-id",
  epochId: "22222222-2222-4222-8222-222222222222",
  deviceId: "33333333-3333-4333-8333-333333333333",
  signerId: "44444444-4444-4444-8444-444444444444",
};
// Independently emitted by SPEC/fixtures/e2ee-public-device-approval.c.
const expected = Buffer.from(
  "SkUyQwEAAAAHZml4dHVyZQAAACNqYXp6LmUyZWUucHVibGljLWRldmljZS1hcHByb3ZhbC52MQAAAAdhY2NvdW50AAAACmFjY291bnQtaWQAAAAeX19lMmVlX3B1YmxpY19kZXZpY2VfYXBwcm92YWxzAAAAJDExMTExMTExLTExMTEtNDExMS04MTExLTExMTExMTExMTExMQAAADdhcHByb3ZhbC1zaWduYXR1cmU6NDQ0NDQ0NDQtNDQ0NC00NDQ0LTg0NDQtNDQ0NDQ0NDQ0NDQ0AAAAJDIyMjIyMjIyLTIyMjItNDIyMi04MjIyLTIyMjIyMjIyMjIyMgAAACQzMzMzMzMzMy0zMzMzLTQzMzMtODMzMy0zMzMzMzMzMzMzMzM=",
  "base64",
);

it("binds every public approval coordinate without private handshake or delivery bytes", async () => {
  const bytes = publicDeviceApprovalBytes("fixture", approval);
  expect(bytes).toEqual(new Uint8Array(expected));
  const seed = Buffer.from(
    "9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60",
    "hex",
  );
  const publicKey = Buffer.from(
    "d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a",
    "hex",
  );
  const privateKey = createPrivateKey({
    key: Buffer.concat([Buffer.from("302e020100300506032b657004220420", "hex"), seed]),
    format: "der",
    type: "pkcs8",
  });
  const header = Buffer.from("4a45324501106a617a7a2e736f6469756d2e7369676e00000001", "hex");
  const signature = Buffer.concat([
    header,
    sign(null, Buffer.concat([header, expected]), privateKey),
  ]);
  for (const adapter of [await createBrowserDeviceSigner(), await createNativeDeviceSigner()]) {
    expect(await adapter.sign(Buffer.concat([seed, publicKey]), bytes)).toEqual(
      new Uint8Array(signature),
    );
    expect(await adapter.verify(publicKey, bytes, signature)).toBe(true);
    expect(
      await adapter.verify(publicKey, publicDeviceApprovalBytes("other-app", approval), signature),
    ).toBe(false);
    for (const field of ["id", "epochId", "deviceId", "signerId", "accountId"] as const) {
      const changed = { ...approval, [field]: "55555555-5555-4555-8555-555555555555" };
      expect(
        await adapter.verify(publicKey, publicDeviceApprovalBytes("fixture", changed), signature),
      ).toBe(false);
    }
  }
  for (const field of ["id", "epochId", "deviceId", "signerId", "accountId"] as const)
    expect(() => publicDeviceApprovalBytes("fixture", { ...approval, [field]: "" })).toThrow();
});
