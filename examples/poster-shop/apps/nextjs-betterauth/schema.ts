import { schema as s } from "jazz-tools";
import { schema as betterAuthSchema } from "./schema-better-auth/schema";

const schema = {
  ...betterAuthSchema,
  canvases: s.table(
    { title: s.string(), width: s.int(), height: s.int() },
    {
      canvasMembersViaCanvas: s.reverse("canvasMembers", "canvas"),
      layersViaCanvas: s.reverse("layers", "canvas"),
      assetsViaCanvas: s.reverse("assets", "canvas"),
      shapesViaCanvas: s.reverse("shapes", "canvas"),
      cursorsViaCanvas: s.reverse("cursors", "canvas"),
      checkpointsViaCanvas: s.reverse("checkpoints", "canvas"),
    },
  ),
  canvasMembers: s
    .table(
      {
        canvasId: s.uuid(),
        // Jazz account ids are stable across linked external identities.
        memberAuthor: s.uuid(),
        role: s.enum("viewer", "editor", "admin"),
      },
      { canvas: s.rel("canvases", "canvasId") },
    )
    .indexOnly(["canvasId", "memberAuthor"]),
  // Every live canvas view is parent-scoped and ordered. Keep those indexes in
  // the app schema rather than relying on a renderer-side sort or scan.
  layers: s
    .table(
      {
        canvasId: s.uuid(),
        name: s.string(),
        zIndex: s.int(),
        visible: s.boolean(),
      },
      { canvas: s.rel("canvases", "canvasId"), shapesViaLayer: s.reverse("shapes", "layer") },
    )
    .indexOnly(["canvasId", "zIndex"]),
  // Asset bytes deliberately remain outside the canvas listing projection.
  // fileId is the future large-value/blob locator (#1833, #1839, #1844); this
  // metadata row is useful even when the bytes are not locally available.
  assets: s
    .table(
      {
        canvasId: s.uuid(),
        name: s.string(),
        mimeType: s.string(),
        byteLength: s.int(),
        fileId: s.string().optional(),
      },
      { canvas: s.rel("canvases", "canvasId"), shapesViaAsset: s.reverse("shapes", "asset") },
    )
    .indexOnly(["canvasId", "name"]),
  shapes: s
    .table(
      {
        canvasId: s.uuid(),
        layerId: s.uuid(),
        assetId: s.uuid().optional(),
        kind: s.enum("rect", "ellipse", "text", "image"),
        x: s.float(),
        y: s.float(),
        width: s.float(),
        height: s.float(),
        rotation: s.float(),
        zIndex: s.int(),
        text: s.string().optional(),
        fill: s.string(),
      },
      {
        canvas: s.rel("canvases", "canvasId"),
        layer: s.rel("layers", "layerId"),
        asset: s.rel("assets", "assetId"),
      },
    )
    .indexOnly(["canvasId", "zIndex"])
    .indexOnly(["layerId", "zIndex"]),
  cursors: s
    .table(
      {
        canvasId: s.uuid(),
        author: s.uuid(),
        x: s.float(),
        y: s.float(),
        color: s.string(),
      },
      { canvas: s.rel("canvases", "canvasId") },
    )
    .indexOnly(["canvasId", "author"]),
  // A checkpoint is an immutable, named application history marker. It does
  // not claim branch winner semantics that the core has not specified yet.
  checkpoints: s
    .table(
      { canvasId: s.uuid(), label: s.string(), branch: s.string() },
      { canvas: s.rel("canvases", "canvasId") },
    )
    .indexOnly(["canvasId", "label"]),
};

type AppSchema = s.Schema<typeof schema>;
export const app: s.App<AppSchema> = s.defineApp(schema);
export type Canvas = s.RowOf<typeof app.canvases>;
