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

/**
 * PosterShop authorization (#1926). Every child table spells out its own role
 * predicate against `canvasMembers`; nothing inherits the unconditional
 * canvas insert rule.
 *
 * | table         | read   | insert           | update               | delete          |
 * | ------------- | ------ | ---------------- | -------------------- | --------------- |
 * | canvases      | member | never (server)   | admin                | admin           |
 * | canvasMembers | member | admin            | admin                | admin, self     |
 * | layers        | member | editor, admin    | editor, admin        | editor, admin * |
 * | shapes        | member | editor, admin *  | editor, admin *      | editor, admin * |
 * | assets        | member | editor, admin    | never (immutable)    | editor, admin   |
 * | cursors       | member | own row, member  | own row, member      | own row         |
 * | checkpoints   | member | admin            | never (immutable)    | never           |
 * | canvasInvites | admin  | admin            | never                | admin           |
 *
 * `*` also requires the layer to belong to the same canvas and be unlocked.
 */

// Child rows carry `canvasId`, so each rule below correlates on that column.
const isCanvasMember = (canvasId: s.Uuid) =>
  exists.canvasMembers({ canvasId, memberAuthor: session.user });

const canEditCanvas = (canvasId: s.Uuid) =>
  exists.canvasMembers({ canvasId, memberAuthor: session.user, role: ["editor", "admin"] });

const isCanvasAdmin = (canvasId: s.Uuid) =>
  exists.canvasMembers({ canvasId, memberAuthor: session.user, role: "admin" });

const mine = { author: session.user };

// Shape admission has two independently correlated proofs over the row
// being written: the selected layer belongs to this shape's canvas (and is
// unlocked), and the current user is an editor or admin of that same
// canvas. Keeping both predicates explicit prevents an editor on one canvas
// from attaching a shape to a layer on another.
const canWriteShape = (shape: s.Row<typeof schema, "shapes">) =>
  allOf(
    // A shape may only sit on an unlocked layer of its own canvas. Checking this
    // on the row being written denies both cross-canvas attachment and edits on
    // a locked layer.
    shape.layer.where({ canvasId: shape.canvasId, locked: false }),
    canEditCanvas(shape.canvasId),
  );

export const permissions = definePermissions({
  // Asset bytes are immutable once uploaded; a replacement is a new asset.
  assets: {
    read: (asset) => allowedTo.read(asset.canvas),
    insert: (asset) => canEditCanvas(asset.canvasId),
    update: false,
    delete: (asset) => canEditCanvas(asset.canvasId),
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
  // Invites are visible to, and issued and revoked by, admins only. Redeeming
  // one happens server-side with backend authority (app/api/join).
  canvasInvites: {
    read: (invite) => isCanvasAdmin(invite.canvasId),
    insert: (invite) => isCanvasAdmin(invite.canvasId),
    update: false,
    delete: (invite) => isCanvasAdmin(invite.canvasId),
  },
  canvasMembers: {
    read: (member) => allowedTo.read(member.canvas),
    // Admins may add members directly; everyone else joins through the invite
    // route, which inserts the membership with backend authority.
    insert: (member) => allowedTo.update(member.canvas),
    update: (member) => allowedTo.update(member.canvas),
    delete: (member) => anyOf(allowedTo.update(member.canvas), { memberAuthor: session.user }),
  },
  canvases: {
    read: (canvas) => isCanvasMember(canvas.id),
    // Canvases (and their first admin membership) are only ever created by the
    // server-side bootstrap in src/lib/bootstrap.ts, which writes with backend
    // authority and bypasses these rules. No client may create a canvas, so
    // there is no unconditional insert rule for child tables to inherit.
    insert: false,
    update: (canvas) => isCanvasAdmin(canvas.id),
    delete: (canvas) => isCanvasAdmin(canvas.id),
  },
  // Checkpoints are admin-owned, immutable named snapshots, not cursor history.
  checkpoints: {
    read: (checkpoint) => allowedTo.read(checkpoint.canvas),
    insert: (checkpoint) => isCanvasAdmin(checkpoint.canvasId),
    update: false,
    delete: false,
  },
  // Presence is replaceable ephemera owned by its author. Any member, viewers
  // included, may publish exactly their own cursor on a canvas they belong
  // to; nobody may write or remove another member's cursor.
  cursors: {
    read: (cursor) => allowedTo.read(cursor.canvas),
    insert: (cursor) => allOf(mine, isCanvasMember(cursor.canvasId)),
    updateIf: mine,
    updateTo: (cursor) => allOf(mine, isCanvasMember(cursor.canvasId)),
    delete: mine,
  },
  // Layers: editors and admins create, rename, reorder, hide and lock them.
  // Both the old and the new row must belong to a canvas the user edits, so a
  // layer cannot be moved onto someone else's canvas.
  layers: {
    read: (layer) => allowedTo.read(layer.canvas),
    insert: (layer) => canEditCanvas(layer.canvasId),
    update: (layer) => canEditCanvas(layer.canvasId),
    delete: (layer) => allOf(canEditCanvas(layer.canvasId), { locked: false }),
  },
  shapes: {
    read: (shape) => allowedTo.read(shape.canvas),
    insert: canWriteShape,
    update: canWriteShape,
    delete: canWriteShape,
  },
});
