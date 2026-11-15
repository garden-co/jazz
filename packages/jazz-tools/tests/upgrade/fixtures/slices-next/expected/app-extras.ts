// What schema.ts exported besides the schema, moved here by `jazz-tools upgrade`.
// The schema itself is jazz.app.json; its types are in jazz.generated.ts.
import type { RowOf } from "jazz-tools";
import type { app, CardInsert } from "./jazz.generated.js";

export const MAX_ITEMS = 50;
export type CardWithBoard = RowOf<ReturnType<typeof app.cards.include<{ board: true }>>>;
export type CardDraft = CardInsert & { draft: true };
