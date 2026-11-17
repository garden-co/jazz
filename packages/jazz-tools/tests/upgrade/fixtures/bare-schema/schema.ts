// Root schema assembles tables defined in other modules; app code imports through src/db (barrel).
import { schema } from "jazz-tools";
import { notes } from "./src/db/tables/notes.js";
import { boards } from "./src/db/tables/boards.js";
import { migrations } from "./migrations/index.js";
const sliceable = schema.defineSliceableApp({ notes, boards }, { migrations });
export const app = sliceable.slice("notes", "boards");
export const notesOnly = sliceable.slice("notes");
