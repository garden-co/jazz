// Root schema assembles tables defined in other modules; app code imports through src/db (barrel).
import { schema as s } from "jazz-tools";
import { notes } from "./src/db/tables/notes.js";
import { boards } from "./src/db/tables/boards.js";
export const schema = s.defineSchema({ notes, boards });

const { definePermissions, session } = schema;

const ownerIdIsMe = { owner_id: session.user };

export const permissions = definePermissions({
  boards: {
    read: ownerIdIsMe,
    insert: ownerIdIsMe,
    update: false,
    delete: false,
  },
  notes: {
    read: ownerIdIsMe,
    insert: ownerIdIsMe,
    update: ownerIdIsMe,
    delete: ownerIdIsMe,
  },
});
