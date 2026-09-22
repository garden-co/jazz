import { describe, expect, it, vi } from "vitest";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import type { MutationErrorEvent } from "../runtime/client.js";
import { deliverMutationErrorToAttachedPeers } from "./mutation-error-delivery.js";

const rejected = {
  code: "permission_denied",
  reason: "write rejected by policy",
  transaction: {
    transactionId: "rejected-transaction",
    kind: "mergeable",
    sealed: true,
    latestSettlement: {
      kind: "rejected",
      transactionId: "rejected-transaction",
      code: "permission_denied",
      reason: "write rejected by policy",
    },
  },
} as MutationErrorEvent;

describe("worker mutation-error delivery", () => {
  it("keeps the worker rejection path stateless", () => {
    const workerCore = readFileSync(
      fileURLToPath(new URL("./jazz-broker-worker-core.ts", import.meta.url)),
      "utf8",
    );

    expect(workerCore).not.toContain("pendingMutationErrors");
    expect(workerCore).toContain(
      "deliverMutationErrorToAttachedPeers(context.peers.values(), event",
    );
  });

  it("does not retain an error observed without peers for a later peer", () => {
    const peers = new Map<string, { id: string }>();
    const deliver = vi.fn();

    deliverMutationErrorToAttachedPeers(peers.values(), rejected, deliver);
    peers.set("replacement", { id: "replacement" });

    expect(deliver).not.toHaveBeenCalled();
  });

  it("delivers once to each peer attached when the error is observed", () => {
    const first = { id: "first" };
    const second = { id: "second" };
    const deliver = vi.fn();

    deliverMutationErrorToAttachedPeers([first, second], rejected, deliver);

    expect(deliver).toHaveBeenCalledTimes(2);
    expect(deliver).toHaveBeenNthCalledWith(1, first, rejected);
    expect(deliver).toHaveBeenNthCalledWith(2, second, rejected);
  });
});
