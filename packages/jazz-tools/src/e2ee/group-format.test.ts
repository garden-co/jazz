import { expect, it } from "vitest";
import { groupRootBytes, groupDeliveryBytes } from "./group-format.js";

const root = {
  id: "11111111-1111-4111-8111-111111111111",
  epochId: "22222222-2222-4222-8222-222222222222",
  deviceId: "33333333-3333-4333-8333-333333333333",
  accountEpochId: "44444444-4444-4444-8444-444444444444",
  accountId: "account-id",
  mechanism: "test",
  version: 1,
  verification: new Uint8Array([0xaa]),
  signature: new Uint8Array(),
};
const delivery = {
  id: "55555555-5555-4555-8555-555555555555",
  groupId: root.id,
  epochId: root.epochId,
  senderAccountId: root.accountId,
  senderDeviceId: root.deviceId,
  recipientAccountId: root.accountId,
  recipientDeviceId: "66666666-6666-4666-8666-666666666666",
  envelope: new Uint8Array([0xaa, 0xbb]),
};

// Independently emitted by SPEC/fixtures/e2ee-group.c, not this serializer.
it("matches the independent initial group root frame", () => {
  const expected = Buffer.from(
    "AAABI0pFMkMBAAAAB2ZpeHR1cmUAAAASamF6ei5lMmVlLmdyb3VwLnYxAAAABWdyb3VwAAAAJDExMTExMTExLTExMTEtNDExMS04MTExLTExMTExMTExMTExMQAAAA1fX2UyZWVfZ3JvdXBzAAAAJDExMTExMTExLTExMTEtNDExMS04MTExLTExMTExMTExMTExMQAAAGNbInJvb3QiLCJhY2NvdW50LWlkIiwiMzMzMzMzMzMtMzMzMy00MzMzLTgzMzMtMzMzMzMzMzMzMzMzIiwiNDQ0NDQ0NDQtNDQ0NC00NDQ0LTg0NDQtNDQ0NDQ0NDQ0NDQ0Il0AAAAkMjIyMjIyMjItMjIyMi00MjIyLTgyMjItMjIyMjIyMjIyMjIyAAAAAAAAAA9KRTJFAQR0ZXN0AAAAAao=",
    "base64",
  );
  expect(groupRootBytes("fixture", root)).toEqual(new Uint8Array(expected));
});

it("matches the independent group delivery frame", () => {
  const expected = Buffer.from(
    "AAABXEpFMkMBAAAAB2ZpeHR1cmUAAAASamF6ei5lMmVlLmdyb3VwLnYxAAAABWdyb3VwAAAAJDExMTExMTExLTExMTEtNDExMS04MTExLTExMTExMTExMTExMQAAAA1fX2UyZWVfZ3JvdXBzAAAAJDExMTExMTExLTExMTEtNDExMS04MTExLTExMTExMTExMTExMQAAAGdbImRlbGl2ZXJ5IiwiNTU1NTU1NTUtNTU1NS00NTU1LTg1NTUtNTU1NTU1NTU1NTU1IiwiYWNjb3VudC1pZCIsIjMzMzMzMzMzLTMzMzMtNDMzMy04MzMzLTMzMzMzMzMzMzMzMyJdAAAAJDIyMjIyMjIyLTIyMjItNDIyMi04MjIyLTIyMjIyMjIyMjIyMgAAADVbImFjY291bnQtaWQiLCI2NjY2NjY2Ni02NjY2LTQ2NjYtODY2Ni02NjY2NjY2NjY2NjYiXQAAAAKquw==",
    "base64",
  );
  expect(groupDeliveryBytes("fixture", root, delivery)).toEqual(new Uint8Array(expected));
});
