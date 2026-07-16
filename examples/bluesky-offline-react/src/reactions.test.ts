import { describe, expect, it } from "vitest";
import { nextReactionIntent } from "./reactions.js";

describe("offline reaction intentions", () => {
  it("collapses a queued repost followed by an undo back to the synced state", () => {
    const repost = nextReactionIntent(false);
    expect(repost).toEqual({ active: true, syncedActive: false, keepPending: true });

    const undo = nextReactionIntent(repost.active, repost);
    expect(undo).toEqual({ active: false, syncedActive: false, keepPending: false });
  });

  it("queues an unrepost when the PDS currently contains the repost", () => {
    expect(nextReactionIntent(true)).toEqual({
      active: false,
      syncedActive: true,
      keepPending: true,
    });
  });
});
