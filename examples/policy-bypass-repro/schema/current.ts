import { table, col } from "jazz-tools";

table("owned_items", {
  title: col.string(),
  ownerId: col.string(),
});
