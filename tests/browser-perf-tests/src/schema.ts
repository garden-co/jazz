import { co, setDefaultSchemaPermissions, z } from "jazz-tools";
import { TodoProject } from "./scenarios/todo/schema";
import { GridRoot } from "./scenarios/grid/schema";

// Re-export scenario schemas for convenience
export { Task, TodoProject } from "./scenarios/todo/schema";
export { PixelCell, GridRoot } from "./scenarios/grid/schema";

setDefaultSchemaPermissions({
  onInlineCreate: "sameAsContainer",
});

/**
 * Combined account root supporting both Todo and Grid scenarios
 */
export const AppAccountRoot = co
  .map({
    // Todo scenario data
    projects: co.list(TodoProject),
    profilingEnabled: z.boolean(),
    // Grid scenario data
    grids: co.list(GridRoot),
  })
  .withPermissions({
    onInlineCreate: "newGroup",
  });

export const AppAccount = co
  .account({
    profile: co.profile(),
    root: AppAccountRoot,
  })
  .withMigration(async (account) => {
    if (!account.$jazz.has("root")) {
      account.$jazz.set("root", {
        projects: [],
        profilingEnabled: false,
        grids: [],
      });
    }
  });
