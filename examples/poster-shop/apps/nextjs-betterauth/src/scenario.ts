export const POSTER_SHOP_FIXTURE_VERSION = 2;
/**
 * A framework-neutral, deterministic workload contract for the app and its
 * topology receipt. "checkpoint" is a durable admin marker, not a request to
 * select or merge a branch winner; that semantics is intentionally deferred.
 */
export const posterShopScenario = {
  id: "poster-shop.canvas-fanout-recovery",
  fixtureVersion: POSTER_SHOP_FIXTURE_VERSION,
  operations: [
    "invite-editors",
    "concurrent-shape-insert",
    "publish-cursors",
    "save-checkpoint",
    "offline-replay",
    "revoke-editor",
  ],
  queries: ["canvas-layers", "ordered-shapes", "asset-metadata", "cursor-fanout", "checkpoints"],
  faults: ["authorization", "disconnect", "reconnect"],
  soak: { editors: 8, transformsPerEditor: 40, cursorHz: 20, rounds: 3 },
} as const;
