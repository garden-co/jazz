import { useAll } from "@jazz/react";
import { app } from "../generated/client.js";
import { TaskList } from "./TaskList.js";

//#region subscription-sharing
// Multiple components subscribing to the same query share a single subscription.
// The _queryKey on query builders enables structural equality comparison.

function ParentComponent() {
  return (
    <>
      {/* These components share a single subscription because they use the same query */}
      <TaskList /> {/* app.tasks.where({ isCompleted: false }) */}
      <TaskList /> {/* Same query = same subscription */}
    </>
  );
}
//#endregion

export { ParentComponent };
