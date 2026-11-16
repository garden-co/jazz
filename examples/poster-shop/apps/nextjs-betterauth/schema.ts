import { schema as s } from "jazz-tools";
import { tables as betterAuthSchema } from "./schema-better-auth/schema";

export const schema = s.defineSchema({
  ...betterAuthSchema,
  canvases: s.table(
    { title: s.string(), width: s.int(), height: s.int() },
    {
      canvasMembersViaCanvas: s.reverse("canvasMembers", "canvas"),
      canvasInvitesViaCanvas: s.reverse("canvasInvites", "canvas"),
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
  // An admin-issued invite link. Redeeming one is a server-side action
  // (app/api/join) that checks the token with backend authority and adds a
  // membership; clients never write their own membership row. The token only
  // ever travels in a URL fragment, and a single-use invite is deleted in the
  // same serializable transaction that admits its first redeemer.
  canvasInvites: s
    .table(
      {
        canvasId: s.uuid(),
        token: s.string(),
        role: s.enum("viewer", "editor"),
        singleUse: s.boolean().default(false),
      },
      { canvas: s.rel("canvases", "canvasId") },
    )
    .indexOnly(["canvasId", "token"]),
  // Every live canvas view is parent-scoped and ordered. Keep those indexes in
  // the app schema rather than relying on a renderer-side sort or scan.
  layers: s
    .table(
      {
        canvasId: s.uuid(),
        name: s.string(),
        zIndex: s.int(),
        visible: s.boolean(),
        // A locked layer is an authorization fact, not only a UI hint: shape
        // writes on a locked layer are denied by permissions.ts.
        locked: s.boolean().default(false),
      },
      { canvas: s.rel("canvases", "canvasId"), shapesViaLayer: s.reverse("shapes", "layer") },
    )
    .indexOnly(["canvasId", "zIndex"]),
  // `bytes` is a large value written with db.insertStreaming. Listings select
  // only the metadata columns, and the image is read back with typed byte-range
  // selections, so browsing the shelf never materializes every upload.
  assets: s
    .table(
      {
        canvasId: s.uuid(),
        name: s.string(),
        mimeType: s.string(),
        byteLength: s.int(),
        width: s.int(),
        height: s.int(),
        bytes: s.bytes(),
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
    // One call: `indexOnly` replaces any earlier list rather than adding to it.
    .indexOnly(["canvasId", "layerId", "zIndex"]),
  cursors: s
    .table(
      {
        canvasId: s.uuid(),
        author: s.uuid(),
        x: s.float(),
        y: s.float(),
        color: s.string(),
        name: s.string().default(""),
        // Cleared when the pointer leaves the poster, so idle collaborators
        // do not leave a frozen arrow behind.
        active: s.boolean().default(true),
      },
      { canvas: s.rel("canvases", "canvasId") },
    )
    .indexOnly(["canvasId", "author"]),
  // A checkpoint is an immutable, named application snapshot: `snapshot` holds
  // the layers and shapes as they were when it was saved (see
  // src/lib/poster.ts). It does not claim branch winner semantics that the
  // core has not specified yet.
  checkpoints: s
    .table(
      {
        canvasId: s.uuid(),
        label: s.string(),
        snapshot: s.json(),
      },
      { canvas: s.rel("canvases", "canvasId") },
    )
    .indexOnly(["canvasId"]),
});

const { definePermissions, exists, allowedTo, session, anyOf, allOf } = schema;

const isCanvasMember = (canvasId: s.Uuid) =>
  exists.canvasMembers({ canvasId, memberAuthor: session.user, role: ["editor", "admin"] });

const isCanvasMemberAdmin = (canvasId: s.Uuid) =>
  exists.canvasMembers({ canvasId, memberAuthor: session.user, role: "admin" });

const isCanvasMember2 = (canvasId: s.Uuid) =>
  exists.canvasMembers({ canvasId, memberAuthor: session.user });

const authorIsMe = { author: session.user };

export const permissions = definePermissions({
  assets: {
    read: (asset) => allowedTo.read(asset.canvas),
    insert: (asset) => isCanvasMember(asset.canvasId),
    update: false,
    delete: (asset) => isCanvasMember(asset.canvasId),
  },
  better_auth_account: {
    read: false,
    insert: false,
    update: false,
    delete: false,
  },
  better_auth_jwks: {
    read: false,
    insert: false,
    update: false,
    delete: false,
  },
  better_auth_session: {
    read: false,
    insert: false,
    update: false,
    delete: false,
  },
  better_auth_user: {
    read: false,
    insert: false,
    update: false,
    delete: false,
  },
  better_auth_verification: {
    read: false,
    insert: false,
    update: false,
    delete: false,
  },
  canvasInvites: {
    read: (canvasInvite) => isCanvasMemberAdmin(canvasInvite.canvasId),
    insert: (canvasInvite) => isCanvasMemberAdmin(canvasInvite.canvasId),
    update: false,
    delete: (canvasInvite) => isCanvasMemberAdmin(canvasInvite.canvasId),
  },
  canvasMembers: {
    read: (canvasMember) => allowedTo.read(canvasMember.canvas),
    insert: (canvasMember) => allowedTo.update(canvasMember.canvas),
    update: (canvasMember) => allowedTo.update(canvasMember.canvas),
    delete: (canvasMember) =>
      anyOf(allowedTo.update(canvasMember.canvas), { memberAuthor: session.user }),
  },
  canvases: {
    read: (canvas) => isCanvasMember2(canvas.id),
    insert: false,
    update: (canvas) => isCanvasMemberAdmin(canvas.id),
    delete: (canvas) => isCanvasMemberAdmin(canvas.id),
  },
  checkpoints: {
    read: (checkpoint) => allowedTo.read(checkpoint.canvas),
    insert: (checkpoint) => isCanvasMemberAdmin(checkpoint.canvasId),
    update: false,
    delete: false,
  },
  cursors: {
    read: (cursor) => allowedTo.read(cursor.canvas),
    insert: (cursor) => allOf(authorIsMe, isCanvasMember2(cursor.canvasId)),
    updateIf: authorIsMe,
    updateTo: (cursor) => allOf(authorIsMe, isCanvasMember2(cursor.canvasId)),
    delete: authorIsMe,
  },
  layers: {
    read: (layer) => allowedTo.read(layer.canvas),
    insert: (layer) => isCanvasMember(layer.canvasId),
    update: (layer) => isCanvasMember(layer.canvasId),
    delete: (layer) => allOf(isCanvasMember(layer.canvasId), { locked: false }),
  },
  shapes: {
    read: (shape) => allowedTo.read(shape.canvas),
    insert: (shape) =>
      allOf(
        shape.layer.where({ canvasId: shape.canvasId, locked: false }),
        isCanvasMember(shape.canvasId),
      ),
    update: (shape) =>
      allOf(
        shape.layer.where({ canvasId: shape.canvasId, locked: false }),
        isCanvasMember(shape.canvasId),
      ),
    delete: (shape) =>
      allOf(
        shape.layer.where({ canvasId: shape.canvasId, locked: false }),
        isCanvasMember(shape.canvasId),
      ),
  },
});
