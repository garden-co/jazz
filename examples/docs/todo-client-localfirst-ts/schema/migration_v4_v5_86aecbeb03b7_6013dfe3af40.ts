import { migrate, col } from "jazz-tools";

migrate("todos", {
  created_at: col.add().int({ default: 0 }),
  priority: col.add().int({ default: 0 }),
});
