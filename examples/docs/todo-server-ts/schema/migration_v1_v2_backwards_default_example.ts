import { migrate, col } from "jazz-tools";

// Example: dropping a column with a backwards default.
// If you roll back from v2 to v1, Jazz reintroduces the column with this value.
migrate("todos", {
  legacy_priority: col.drop().int({ backwardsDefault: 0 }),
});
