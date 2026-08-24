export const POSTER_SHOP_FIXTURE_VERSION = 1;
export const posterShopScenario = {
  id: "poster-shop.canvas-fanout-recovery",
  fixtureVersion: POSTER_SHOP_FIXTURE_VERSION,
  operations: [
    "invite-editors",
    "batch-transform",
    "publish-cursors",
    "checkpoint-branch",
    "offline-replay",
    "revoke-editor",
  ],
  queries: ["canvas-layers", "ordered-shapes", "cursor-fanout", "branch-history"],
  faults: ["delay", "drop", "partition", "edge-restart", "core-restart"],
  soak: { editors: 8, transformsPerEditor: 40, cursorHz: 20, rounds: 3 },
} as const;
