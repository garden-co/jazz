import { expect, it } from "vitest";
import { publicDeviceApprovalBytes } from "./public-device-approval.js";

it("domain-separates recovery-backed device approvals from ordinary approvals", () => {
  const approval = {
    id: "11111111-1111-4111-8111-111111111111",
    accountId: "account-id",
    epochId: "22222222-2222-4222-8222-222222222222",
    deviceId: "33333333-3333-4333-8333-333333333333",
    signerId: "33333333-3333-4333-8333-333333333333",
    recoveryRootId: "44444444-4444-4444-8444-444444444444",
  };
  const bytes = publicDeviceApprovalBytes("fixture", approval);
  expect(bytes).toEqual(
    new Uint8Array(
      Buffer.from(
        "SkUyQwEAAAAHZml4dHVyZQAAACVqYXp6LmUyZWUucmVjb3ZlcnktZGV2aWNlLWFwcHJvdmFsLnYxAAAAB2FjY291bnQAAAAKYWNjb3VudC1pZAAAAB5fX2UyZWVfcHVibGljX2RldmljZV9hcHByb3ZhbHMAAAAkMTExMTExMTEtMTExMS00MTExLTgxMTEtMTExMTExMTExMTExAAAAN3JlY292ZXJ5LXNpZ25hdHVyZTo0NDQ0NDQ0NC00NDQ0LTQ0NDQtODQ0NC00NDQ0NDQ0NDQ0NDQAAAAkMjIyMjIyMjItMjIyMi00MjIyLTgyMjItMjIyMjIyMjIyMjIyAAAAJDMzMzMzMzMzLTMzMzMtNDMzMy04MzMzLTMzMzMzMzMzMzMzMw==",
        "base64",
      ),
    ),
  );
  expect(bytes).not.toEqual(
    publicDeviceApprovalBytes("fixture", { ...approval, recoveryRootId: null }),
  );
  expect(() =>
    publicDeviceApprovalBytes("fixture", { ...approval, recoveryRootId: "invalid" }),
  ).toThrow();
  expect(() =>
    publicDeviceApprovalBytes("fixture", {
      ...approval,
      signerId: "55555555-5555-4555-8555-555555555555",
    }),
  ).toThrow();
});
