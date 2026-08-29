import { describe, expect, it } from "vitest";
import { createOpenTransactionId, type TxId, type OpenTransactionId } from "./client.js";

describe("transaction identities", () => {
  it("creates canonical UUIDv7 open-transaction ids without coordination", () => {
    const first = createOpenTransactionId();
    const second = createOpenTransactionId();

    expect(first).toMatch(/^[0-9a-f]{12}7[0-9a-f]{3}[89ab][0-9a-f]{15}$/);
    expect(second).not.toBe(first);
  });

  it("keeps mutable and committed identities distinct by construction", () => {
    const acceptOpen = (_id: OpenTransactionId) => undefined;
    const acceptCommitted = (_id: TxId) => undefined;
    const open = createOpenTransactionId();

    acceptOpen(open);
    // @ts-expect-error An open transaction cannot be used where a committed transaction is required.
    acceptCommitted(open);
    // @ts-expect-error A plain string is neither a mutable nor committed transaction identity.
    acceptOpen("019fe3d74db570bbb6fa257229b0a5fe");
  });
});
