import { table, col } from "jazz-tools";

table("instruments", {
  name: col.string(),
  sound: col.bytes(),
  display_order: col.int(),
});

table("jams", {
  created_at: col.timestamp(),
  transport_start: col.timestamp().optional(),
  bpm: col.int(),
  beat_count: col.int(),
});

table("beats", {
  jamId: col.ref("jams"),
  instrumentId: col.ref("instruments"),
  beat_index: col.int(), // 0–15
  placed_by: col.string(), // session user_id
});

table("participants", {
  jamId: col.ref("jams"),
  userId: col.string(),
  display_name: col.string(),
});
