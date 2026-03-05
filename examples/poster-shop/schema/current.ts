import { col, table } from "jazz-tools";
import { z } from "zod";

const Point = z.object({ x: z.number(), y: z.number() });
const Stroke = z.array(Point);

table("users", {
  user_id: col.string(),
  name: col.string(),
  created_at: col.string(),
});

table("canvases", {
  name: col.string(),
  created_at: col.string(),
});

table("strokes", {
  canvas_id: col.ref("canvases"),
  user_id: col.string(),
  points: col.json(Stroke),
  created_at: col.string(),
});
