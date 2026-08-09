import { describe, expect, it } from "vitest";
import { createOpenBatchId, type BatchId, type OpenBatchId } from "./client.js";

describe("batch identities", () => {
  it("creates canonical UUIDv7 open-batch ids without coordination", () => {
    const first = createOpenBatchId();
    const second = createOpenBatchId();

    expect(first).toMatch(/^[0-9a-f]{12}7[0-9a-f]{3}[89ab][0-9a-f]{15}$/);
    expect(second).not.toBe(first);
  });

  it("keeps mutable and committed identities distinct by construction", () => {
    const acceptOpen = (_id: OpenBatchId) => undefined;
    const acceptCommitted = (_id: BatchId) => undefined;
    const open = createOpenBatchId();

    acceptOpen(open);
    // @ts-expect-error An open batch cannot be used where a committed batch is required.
    acceptCommitted(open);
    // @ts-expect-error A plain string is neither a mutable nor committed batch identity.
    acceptOpen("019fe3d74db570bbb6fa257229b0a5fe");
  });
});
