import { expect, it } from "vitest";
import { encodeRecoveryMaterial, recoveryDeliveryContext } from "./recovery-format.js";

it("pins the recovery delivery context independently", () => {
  // SPEC/fixtures/e2ee-recovery-root.c delivery emits this literal context.
  const expected = Buffer.from(
    "SkUyQwEAAAAHZml4dHVyZQAAABVqYXp6LmUyZWUucmVjb3ZlcnkudjEAAAAHYWNjb3VudAAAAAphY2NvdW50LWlkAAAAGl9fZTJlZV9yZWNvdmVyeV9kZWxpdmVyaWVzAAAAJDExMTExMTExLTExMTEtNDExMS04MTExLTExMTExMTExMTExMQAAAAhlbnZlbG9wZQAAACQyMjIyMjIyMi0yMjIyLTQyMjItODIyMi0yMjIyMjIyMjIyMjIAAAAkMzMzMzMzMzMtMzMzMy00MzMzLTgzMzMtMzMzMzMzMzMzMzMz",
    "base64",
  );
  expect(
    recoveryDeliveryContext("fixture", "account-id", {
      id: "11111111-1111-4111-8111-111111111111",
      epochId: "22222222-2222-4222-8222-222222222222",
      rootId: "33333333-3333-4333-8333-333333333333",
    }),
  ).toEqual(new Uint8Array(expected));
});

it("pins the client-only recovery material format without implicit typed-array encoding", () => {
  // Short public fixture bytes pin representation, not cryptographic key validity.
  expect(
    encodeRecoveryMaterial(
      "fixture",
      {
        id: "11111111-1111-4111-8111-111111111111",
        accountId: "account-id",
        epochId: "22222222-2222-4222-8222-222222222222",
        signerId: "33333333-3333-4333-8333-333333333333",
        publicKey: Uint8Array.of(1),
        mechanism: "key",
        version: 1,
        signingPublicKey: Uint8Array.of(2),
        signingMechanism: "sign",
        signingVersion: 1,
      },
      Uint8Array.of(3),
      Uint8Array.of(4),
    ),
  ).toBe(
    '{"format":"jazz-e2ee-recovery-v1","scope":"fixture","rootId":"11111111-1111-4111-8111-111111111111","mechanism":"key","version":1,"publicKey":[1],"privateKey":[3],"signingMechanism":"sign","signingVersion":1,"signingPublicKey":[2],"signingPrivateKey":[4]}',
  );
});
