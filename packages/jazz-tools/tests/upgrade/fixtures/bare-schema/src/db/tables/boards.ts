import { schema as jz } from "jazz-tools";
export const boards = jz.table(
  { title: jz.string(), color: jz.string().optional(), owner_id: jz.uuid() },
  { notes: jz.reverse("notes", "board") },
);
