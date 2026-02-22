import { migrate, col } from "jazz-tools";

// Example: dropping a column with a backwards default.
// Clients still on v1 continue seeing legacy_priority.
// For rows written by v2 clients, the lens supplies this default value.
migrate("todos", {
  legacy_priority: col.drop().int({ backwardsDefault: 0 }),
});
