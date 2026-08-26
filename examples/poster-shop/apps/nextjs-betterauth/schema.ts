import { schema as s } from "jazz-tools";
import { schema as betterAuthSchema } from "./schema-better-auth/schema";

const schema = {
  ...betterAuthSchema,
  canvases: s.table({ title: s.string(), width: s.int(), height: s.int() }),
  canvasMembers: s
    .table({
      canvasId: s.ref("canvases"),
      userId: s.string(),
      role: s.enum("viewer", "editor", "admin"),
    })
    .indexOnly(["canvasId", "userId"]),
  // Every live canvas view is parent-scoped and ordered. Keep those indexes in
  // the app schema rather than relying on a renderer-side sort or scan.
  layers: s
    .table({
      canvasId: s.ref("canvases"),
      name: s.string(),
      zIndex: s.int(),
      visible: s.boolean(),
    })
    .indexOnly(["canvasId", "zIndex"]),
  // Asset bytes deliberately remain outside the canvas listing projection.
  // fileId is the future large-value/blob locator (#1833, #1839, #1844); this
  // metadata row is useful even when the bytes are not locally available.
  assets: s
    .table({
      canvasId: s.ref("canvases"),
      name: s.string(),
      mimeType: s.string(),
      byteLength: s.int(),
      fileId: s.string().optional(),
    })
    .indexOnly(["canvasId", "name"]),
  shapes: s
    .table({
      canvasId: s.ref("canvases"),
      layerId: s.ref("layers"),
      assetId: s.ref("assets").optional(),
      kind: s.enum("rect", "ellipse", "text", "image"),
      x: s.float(),
      y: s.float(),
      width: s.float(),
      height: s.float(),
      rotation: s.float(),
      zIndex: s.int(),
      text: s.string().optional(),
      fill: s.string(),
    })
    .indexOnly(["canvasId", "zIndex"])
    .indexOnly(["layerId", "zIndex"]),
  cursors: s
    .table({
      canvasId: s.ref("canvases"),
      userId: s.string(),
      x: s.float(),
      y: s.float(),
      color: s.string(),
    })
    .indexOnly(["canvasId", "userId"]),
  // A checkpoint is an immutable, named application history marker. It does
  // not claim branch winner semantics that the core has not specified yet.
  checkpoints: s
    .table({ canvasId: s.ref("canvases"), label: s.string(), branch: s.string() })
    .indexOnly(["canvasId", "label"]),
};

type AppSchema = s.Schema<typeof schema>;
export const app: s.App<AppSchema> = s.defineApp(schema);
export type Canvas = s.RowOf<typeof app.canvases>;
