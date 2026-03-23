// #region backwards-default-example
import { migrate, col } from "jazz-tools";

// Clients still on v1 continue seeing priority.
// For rows written by v2 clients, the lens supplies this default value.
migrate("todos", {
  priority: col.drop().int({ backwardsDefault: 0 }),
});
// #endregion backwards-default-example
