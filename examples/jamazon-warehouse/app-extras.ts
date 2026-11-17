// What schema.ts exported besides the schema, moved here by `jazz-tools upgrade`.
// The schema is schema.ts (for `generate` only); its types are in jazz.generated.ts.

/**
 * Stock rows may not declare a reorder level above this cap. The invariant is
 * what makes the stock-level report's indexed candidate read complete: any
 * row with `on_hand < reorder_level` also has `on_hand < REORDER_LEVEL_CAP`.
 * See `stockLevelCandidates` in src/warehouse.ts and #1864.
 */
export const REORDER_LEVEL_CAP = 100;
