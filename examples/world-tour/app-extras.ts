// What schema.ts exported besides the schema, moved here by `jazz-tools upgrade`.
// The schema itself is jazz.app.json; its types are in jazz.generated.ts.
import type { RowOf } from "jazz-tools";
import { app } from "./jazz.generated.js";

const stopWithVenueQuery = app.stops.include({ venue: true });
export type StopWithVenue = RowOf<typeof stopWithVenueQuery>;
export type StopStatus = StopWithVenue["status"];
