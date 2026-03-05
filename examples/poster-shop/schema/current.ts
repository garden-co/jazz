import { col, table } from "jazz-tools";

table("canvases", {
  name: col.string(),
  created_at: col.string(),
});

table("strokes", {
  canvas_id: col.ref("canvases"),
  user_id: col.string(),
  points: col.json(),
  created_at: col.string(),
});
