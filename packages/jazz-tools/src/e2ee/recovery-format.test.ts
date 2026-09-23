import { expect, it } from "vitest";
import { recoveryRootBytes } from "./recovery-format.js";

// SPEC/fixtures/e2ee-recovery-root.c emits these bytes independently.
it("pins the recovery root's account, author, epoch and independent public keys", () => {
  const expected = Buffer.from(
    "AAAA4kpFMkMBAAAAB2ZpeHR1cmUAAAAVamF6ei5lMmVlLnJlY292ZXJ5LnYxAAAAB2FjY291bnQAAAAKYWNjb3VudC1pZAAAABVfX2UyZWVfcmVjb3Zlcnlfcm9vdHMAAAAkMTExMTExMTEtMTExMS00MTExLTgxMTEtMTExMTExMTExMTExAAAAL1sicm9vdCIsIjMzMzMzMzMzLTMzMzMtNDMzMy04MzMzLTMzMzMzMzMzMzMzMyJdAAAAJDIyMjIyMjIyLTIyMjItNDIyMi04MjIyLTIyMjIyMjIyMjIyMgAAAAAAAAAPSkUyRQEEc2lnbgAAAAGqAAAAD0pFMkUBA2tleQAAAAK7zA==",
    "base64",
  );
  expect(
    recoveryRootBytes("fixture", {
      id: "11111111-1111-4111-8111-111111111111",
      accountId: "account-id",
      epochId: "22222222-2222-4222-8222-222222222222",
      signerId: "33333333-3333-4333-8333-333333333333",
      signingMechanism: "sign",
      signingVersion: 1,
      signingPublicKey: new Uint8Array([0xaa]),
      mechanism: "key",
      version: 2,
      publicKey: new Uint8Array([0xbb, 0xcc]),
    }),
  ).toEqual(new Uint8Array(expected));
});
