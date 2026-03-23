import { migrate, col } from "jazz-tools";

migrate("todos", {
  parent_id: col.rename("parent"),
  project_id: col.rename("project"),
});
