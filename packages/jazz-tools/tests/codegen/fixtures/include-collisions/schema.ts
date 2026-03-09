import { col, table } from "../../../../src/dsl.js";

export function buildSchema() {
  table("users", { name: col.string() });
  table("projects", { name: col.string() });
  table("todos", {
    owner: col.ref("users"),
    project_id: col.ref("projects"),
    title: col.string(),
  });
}
