import { col, table } from "../../../../src/dsl.js";

export function buildSchema() {
  table("teams", { legacy_id: col.string() });
  table("resources", { kind: col.enum("branding") });
  table("resource_access_edges", {
    resource: col.ref("resources"),
    team: col.ref("teams"),
    grant_role: col.enum("viewer", "editor", "manager"),
  });
  table("brandings", {
    resource: col.ref("resources"),
    name: col.string(),
  });
}
