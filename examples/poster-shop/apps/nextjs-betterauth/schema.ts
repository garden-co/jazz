import { schema as s } from "jazz-tools";

const schema = {
  canvases: s.table({ title: s.string(), width: s.int(), height: s.int() }),
  canvasMembers: s.table({
    canvasId: s.ref("canvases"),
    userId: s.string(),
    role: s.enum("viewer", "editor", "admin"),
  }),
  layers: s.table({
    canvasId: s.ref("canvases"),
    name: s.string(),
    zIndex: s.int(),
    visible: s.boolean(),
  }),
  assets: s.table({
    canvasId: s.ref("canvases"),
    name: s.string(),
    mimeType: s.string(),
    byteLength: s.int(),
    fileId: s.string().optional(),
  }),
  shapes: s.table({
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
  }),
  cursors: s.table({
    canvasId: s.ref("canvases"),
    userId: s.string(),
    x: s.float(),
    y: s.float(),
    color: s.string(),
  }),
  checkpoints: s.table({ canvasId: s.ref("canvases"), label: s.string(), branch: s.string() }),
};

type AppSchema = s.Schema<typeof schema>;
export const app: s.App<AppSchema> = s.defineApp(schema);
export type Canvas = s.RowOf<typeof app.canvases>;
