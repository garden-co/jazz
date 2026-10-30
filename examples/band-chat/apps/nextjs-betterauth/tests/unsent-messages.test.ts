import { describe, expect, it } from "vitest";
import { PersistedWriteRejectedError } from "jazz-tools";
import { UnsentMessages, type TrackedWrite } from "../src/lib/unsent-messages";

type TxId = ConstructorParameters<typeof PersistedWriteRejectedError>[0];

/** A write whose global wait the test settles by hand. */
function pendingWrite(txId: string) {
  let settle!: { resolve: () => void; reject: (cause: unknown) => void };
  const waited = new Promise<void>((resolve, reject) => {
    settle = { resolve, reject };
  });
  const write: TrackedWrite = { txId: Promise.resolve(txId), wait: () => waited };
  return { write, ...settle };
}

const flush = () => new Promise((resolve) => setTimeout(resolve, 0));
const outgoing = { roomId: "room-1", roomName: "Rehearsal", text: "Can we push it to 8?" };

describe("UnsentMessages", () => {
  it("keeps a message the server rejected, with its room and text", async () => {
    const unsent = new UnsentMessages();
    let changes = 0;
    unsent.subscribe(() => changes++);
    const { write, reject } = pendingWrite("tx-1");
    unsent.track(write, outgoing);
    reject(
      new PersistedWriteRejectedError("tx-1" as unknown as TxId, "permission_denied", "denied"),
    );
    await flush();
    expect(unsent.getSnapshot()).toMatchObject([{ ...outgoing, reason: "denied" }]);
    expect(changes).toBe(1);
  });

  // A queued message outlives its wait: a long outage ends the wait without
  // a verdict, and the rejection then arrives as a mutation error once the
  // sender, removed from the room meanwhile, is back online.
  it("matches a rejection that arrives after the wait gave up", async () => {
    const unsent = new UnsentMessages();
    const { write, reject } = pendingWrite("tx-2");
    unsent.track(write, outgoing);
    reject(new Error("[object Event]"));
    await flush();
    expect(unsent.getSnapshot()).toEqual([]);

    const matched = unsent.reportMutationError({
      reason: "denied",
      transaction: { transactionId: "tx-2" },
    });
    expect(matched).toBe(true);
    expect(unsent.getSnapshot()).toMatchObject([{ ...outgoing, reason: "denied" }]);
  });

  it("forgets a message once the server accepts it", async () => {
    const unsent = new UnsentMessages();
    const { write, resolve } = pendingWrite("tx-3");
    unsent.track(write, outgoing);
    resolve();
    await flush();
    expect(
      unsent.reportMutationError({ reason: "denied", transaction: { transactionId: "tx-3" } }),
    ).toBe(false);
    expect(unsent.getSnapshot()).toEqual([]);
  });

  it("hands a rejected message back for its draft, and dismisses its notice", async () => {
    const unsent = new UnsentMessages();
    const drafts: string[] = [];
    unsent.onUnsent((message) => drafts.push(`${message.roomId}: ${message.text}`));
    const { write, reject } = pendingWrite("tx-4");
    unsent.track(write, outgoing);
    reject(
      new PersistedWriteRejectedError("tx-4" as unknown as TxId, "permission_denied", "denied"),
    );
    await flush();
    expect(drafts).toEqual(["room-1: Can we push it to 8?"]);

    const [notice] = unsent.getSnapshot();
    unsent.dismiss(notice!.id);
    expect(unsent.getSnapshot()).toEqual([]);
  });
});
