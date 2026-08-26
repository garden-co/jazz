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
    "save-checkpoint",
    "offline-replay",
    "persistent-reopen",
    "revoke-editor",
  ],
  queries: ["canvas-layers", "ordered-shapes", "shape-window", "checkpoints"],
  faults: ["authorization", "disconnect", "restart", "reconnect"],
  soak: { editors: 8, transformsPerEditor: 40, rounds: 3 },
} as const;
