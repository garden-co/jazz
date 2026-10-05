import { expect, it } from "vitest";
import { successorSigningBytes, publicSuccessorSigningBytes } from "./account-successor.js";

const row = {
  id: "11111111-1111-4111-8111-111111111111",
  predecessor: "22222222-2222-4222-8222-222222222222",
  epochId: "33333333-3333-4333-8333-333333333333",
  signerId: "44444444-4444-4444-8444-444444444444",
  action: "remove-device" as const,
  removedDeviceId: "55555555-5555-4555-8555-555555555555",
  accountId: "account-id",
  membership: new TextEncoder().encode('["44444444-4444-4444-8444-444444444444"]'),
  revision: new TextEncoder().encode("[]"),
  verification: Uint8Array.of(2),
  history: Uint8Array.of(3),
  deliveries: new TextEncoder().encode('[["44444444-4444-4444-8444-444444444444",[1]]]'),
};
const retirement = {
  ...row,
  action: "retire-recovery-root" as const,
  removedDeviceId: undefined,
  retiredRecoveryRootId: "66666666-6666-4666-8666-666666666666",
};

it("matches the independently framed C successor fixture including its predecessor", () => {
  // Producer: crates/jazz/SPEC/fixtures/e2ee-account-successor.c; no Jazz codecs.
  const expected =
    "SkUyQwEAAAAHZml4dHVyZQAAAB5qYXp6LmUyZWUuYWNjb3VudC1zdWNjZXNzb3IudjEAAAAHYWNjb3VudAAAAAphY2NvdW50LWlkAAAAGV9fZTJlZV9hY2NvdW50X3N1Y2Nlc3NvcnMAAAAkMTExMTExMTEtMTExMS00MTExLTgxMTEtMTExMTExMTExMTExAAAACXNpZ25hdHVyZQAAACQzMzMzMzMzMy0zMzMzLTQzMzMtODMzMy0zMzMzMzMzMzMzMzMAAAAkNDQ0NDQ0NDQtNDQ0NC00NDQ0LTg0NDQtNDQ0NDQ0NDQ0NDQ0AAAAJDIyMjIyMjIyLTIyMjItNDIyMi04MjIyLTIyMjIyMjIyMjIyMgAAAA1yZW1vdmUtZGV2aWNlAAAAJDU1NTU1NTU1LTU1NTUtNDU1NS04NTU1LTU1NTU1NTU1NTU1NQAAAChbIjQ0NDQ0NDQ0LTQ0NDQtNDQ0NC04NDQ0LTQ0NDQ0NDQ0NDQ0NCJdAAAAAltdAAAAAQIAAAABAwAAAC5bWyI0NDQ0NDQ0NC00NDQ0LTQ0NDQtODQ0NC00NDQ0NDQ0NDQ0NDQiLFsxXV1d";
  expect(Buffer.from(successorSigningBytes("fixture", row)).toString("base64")).toBe(expected);
  const retirementBytes = successorSigningBytes("fixture", retirement);
  expect(retirementBytes).not.toEqual(Buffer.from(expected, "base64"));
  expect(
    successorSigningBytes("fixture", {
      ...retirement,
      retiredRecoveryRootId: "77777777-7777-4777-8777-777777777777",
    }),
  ).not.toEqual(retirementBytes);
  expect(successorSigningBytes("fixture", { ...row, predecessor: row.id })).not.toEqual(
    Buffer.from(expected, "base64"),
  );
});

it("rejects ambiguous membership, mismatched recipients and reused epochs", () => {
  expect(() => successorSigningBytes("fixture", { ...row, predecessor: row.epochId })).toThrow();
  expect(() =>
    successorSigningBytes("fixture", { ...row, membership: new TextEncoder().encode("[]") }),
  ).toThrow();
  expect(() =>
    successorSigningBytes("fixture", { ...row, revision: new TextEncoder().encode("[ ]") }),
  ).toThrow();
  expect(() => successorSigningBytes("fixture", { ...row, removedDeviceId: undefined })).toThrow();
  expect(() =>
    publicSuccessorSigningBytes("fixture", {
      ...row,
      retiredRecoveryRootId: row.removedDeviceId,
    }),
  ).toThrow();
});

it("pins the key-free public successor frame independently and binds every field", () => {
  // Producer: SPEC/fixtures/e2ee-public-account-successor.c; no Jazz codecs.
  const expected = Buffer.from(
    "SkUyQwEAAAAHZml4dHVyZQAAACVqYXp6LmUyZWUucHVibGljLWFjY291bnQtc3VjY2Vzc29yLnYxAAAAB2FjY291bnQAAAAKYWNjb3VudC1pZAAAACBfX2UyZWVfcHVibGljX2FjY291bnRfc3VjY2Vzc29ycwAAACQxMTExMTExMS0xMTExLTQxMTEtODExMS0xMTExMTExMTExMTEAAAAJc2lnbmF0dXJlAAAAJDMzMzMzMzMzLTMzMzMtNDMzMy04MzMzLTMzMzMzMzMzMzMzMwAAACQ0NDQ0NDQ0NC00NDQ0LTQ0NDQtODQ0NC00NDQ0NDQ0NDQ0NDQAAAAkMjIyMjIyMjItMjIyMi00MjIyLTgyMjItMjIyMjIyMjIyMjIyAAAADXJlbW92ZS1kZXZpY2UAAAAkNTU1NTU1NTUtNTU1NS00NTU1LTg1NTUtNTU1NTU1NTU1NTU1AAAAKFsiNDQ0NDQ0NDQtNDQ0NC00NDQ0LTg0NDQtNDQ0NDQ0NDQ0NDQ0Il0AAAACW10=",
    "base64",
  );
  const bytes = publicSuccessorSigningBytes("fixture", row);
  expect(bytes).toEqual(new Uint8Array(expected));
  expect(
    publicSuccessorSigningBytes("fixture", {
      ...row,
      revision: new TextEncoder().encode(JSON.stringify([row.id])),
    }),
  ).not.toEqual(bytes);
  expect(() =>
    publicSuccessorSigningBytes("fixture", {
      ...row,
      revision: new TextEncoder().encode("[ ]"),
    }),
  ).toThrow();
  expect(publicSuccessorSigningBytes("other", row)).not.toEqual(bytes);
  for (const field of [
    "id",
    "accountId",
    "epochId",
    "predecessor",
    "signerId",
    "removedDeviceId",
  ] as const) {
    expect(
      publicSuccessorSigningBytes("fixture", {
        ...row,
        [field]: "66666666-6666-4666-8666-666666666666",
      }),
    ).not.toEqual(bytes);
    expect(() => publicSuccessorSigningBytes("fixture", { ...row, [field]: "" })).toThrow();
  }
  const retirementBytes = publicSuccessorSigningBytes("fixture", retirement);
  expect(retirementBytes).not.toEqual(bytes);
  expect(
    publicSuccessorSigningBytes("fixture", {
      ...retirement,
      retiredRecoveryRootId: "77777777-7777-4777-8777-777777777777",
    }),
  ).not.toEqual(retirementBytes);
  expect(
    publicSuccessorSigningBytes("fixture", { ...row, membership: new TextEncoder().encode("[]") }),
  ).not.toEqual(bytes);
  expect(() =>
    publicSuccessorSigningBytes("fixture", { ...row, predecessor: row.epochId }),
  ).toThrow();
  expect(() =>
    publicSuccessorSigningBytes("fixture", { ...row, membership: new TextEncoder().encode("[ ]") }),
  ).toThrow();
  expect(() =>
    publicSuccessorSigningBytes("fixture", {
      ...row,
      membership: new TextEncoder().encode(JSON.stringify([row.removedDeviceId])),
    }),
  ).toThrow();
  const privateFields = {
    verification: Uint8Array.of(9),
    history: Uint8Array.of(9),
    deliveries: Uint8Array.of(9),
  };
  expect(publicSuccessorSigningBytes("fixture", { ...row, ...privateFields })).toEqual(bytes);
});
