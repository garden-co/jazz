import { col, table } from "jazz-tools";

table("counter_events", {
  actor_id: col.string(),
  created_at: col.string(),
});
