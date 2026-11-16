// What schema.ts exported besides the schema, moved here by `jazz-tools upgrade`.
// The schema is schema.ts (for `generate` only); its types are in jazz.generated.ts.
import type { Turn } from "./jazz.generated";

/** Turns as the UI reads them: with Jazz's own creation time (`$createdAt`). */
export type TimedTurn = Turn & { $createdAt: Date };
