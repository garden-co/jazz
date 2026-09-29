import { definePermissions, type RowContext, type RowRefValue } from "jazz-tools/permissions";
import { app, type Canvas } from "./schema";

type Role = "viewer" | "editor" | "admin";

/**
 * PosterShop authorization (#1926). Every child table spells out its own role
 * predicate against `canvasMembers`; nothing inherits the unconditional
 * canvas insert rule.
 *
 * | table         | read   | insert           | update               | delete          |
 * | ------------- | ------ | ---------------- | -------------------- | --------------- |
 * | layers        | member | editor, admin    | editor, admin        | editor, admin * |
 * | shapes        | member | editor, admin *  | editor, admin *      | editor, admin * |
 * | assets        | member | editor, admin    | never (immutable)    | editor, admin   |
 * | cursors       | member | own row, member  | own row, member      | own row         |
 * | checkpoints   | member | admin            | never (immutable)    | never           |
 * | canvasInvites | admin  | admin            | never                | admin           |
 *
 * `*` also requires the layer to belong to the same canvas and be unlocked.
 */
export default definePermissions(app, ({ policy, session, anyOf, allOf, allowedTo }) => {
  for (const table of [
    policy.better_auth_user,
    policy.better_auth_session,
    policy.better_auth_account,
    policy.better_auth_verification,
    policy.better_auth_jwks,
  ]) {
    table.allowRead.never();
    table.allowInsert.never();
    table.allowUpdate.never();
    table.allowDelete.never();
  }
  const hasRole = (canvas: RowContext<Canvas>, role: Role) =>
    policy.canvasMembers.exists.where({
      canvasId: canvas.id,
      memberAuthor: session.user.account,
      role,
    });
  const canRead = (canvas: RowContext<Canvas>) =>
    policy.canvasMembers.exists.where({ canvasId: canvas.id, memberAuthor: session.user.account });
  const isAdmin = (canvas: RowContext<Canvas>) => hasRole(canvas, "admin");
  // Child rows carry `canvasId`, so each rule below correlates on that column.
  const isMemberOf = (canvasId: RowRefValue) =>
    policy.canvasMembers.exists.where({ canvasId, memberAuthor: session.user.account });
  const canEditCanvas = (canvasId: RowRefValue) =>
    policy.canvasMembers.exists.where({
      canvasId,
      memberAuthor: session.user.account,
      role: { in: ["editor", "admin"] },
    });
  const isAdminOf = (canvasId: RowRefValue) =>
    policy.canvasMembers.exists.where({
      canvasId,
      memberAuthor: session.user.account,
      role: "admin",
    });
  // A shape may only sit on an unlocked layer of its own canvas. Checking this
  // on the row being written denies both cross-canvas attachment and edits on
  // a locked layer.
  const unlockedLayerOnSameCanvas = (shape: { layerId: RowRefValue; canvasId: RowRefValue }) =>
    policy.layers.exists.where({ id: shape.layerId, canvasId: shape.canvasId, locked: false });

  policy.canvases.allowRead.where((canvas) => canRead(canvas));
  // Canvas bootstrap is intentionally unconditional. `allowedTo.insert` is
  // therefore never used for child tables: it would inherit this rule.
  policy.canvases.allowInsert.always();
  policy.canvases.allowUpdate.where((canvas) => isAdmin(canvas));
  policy.canvases.allowDelete.where((canvas) => isAdmin(canvas));
  policy.canvasMembers.allowRead.where(allowedTo.read("canvas"));
  policy.canvasMembers.allowInsert.where((member) =>
    anyOf([
      allowedTo.update("canvas"),
      allOf([
        { memberAuthor: session.user.account, role: "admin" },
        policy.canvases.exists.where({
          id: member.canvasId,
          "$createdBy.account": session.user.account,
        }),
      ]),
    ]),
  );
  policy.canvasMembers.allowUpdate.where(allowedTo.update("canvas"));
  policy.canvasMembers.allowDelete.where(
    anyOf([allowedTo.update("canvas"), { memberAuthor: session.user.account }]),
  );

  // Invites are visible to, and issued and revoked by, admins only. Redeeming
  // one happens server-side with backend authority (app/api/join).
  policy.canvasInvites.allowRead.where((invite) => isAdminOf(invite.canvasId));
  policy.canvasInvites.allowInsert.where((invite) => isAdminOf(invite.canvasId));
  policy.canvasInvites.allowUpdate.never();
  policy.canvasInvites.allowDelete.where((invite) => isAdminOf(invite.canvasId));

  // Layers: editors and admins create, rename, reorder, hide and lock them.
  // Both the old and the new row must belong to a canvas the user edits, so a
  // layer cannot be moved onto someone else's canvas.
  policy.layers.allowRead.where(allowedTo.read("canvas"));
  policy.layers.allowInsert.where((layer) => canEditCanvas(layer.canvasId));
  policy.layers.allowUpdate
    .whereOld((layer) => canEditCanvas(layer.canvasId))
    .whereNew((layer) => canEditCanvas(layer.canvasId));
  policy.layers.allowDelete.where((layer) =>
    allOf([canEditCanvas(layer.canvasId), { locked: false }]),
  );

  // Shape admission has two independently correlated proofs over the row
  // being written: the selected layer belongs to this shape's canvas (and is
  // unlocked), and the current user is an editor or admin of that same
  // canvas. Keeping both predicates explicit prevents an editor on one canvas
  // from attaching a shape to a layer on another.
  const canWriteShape = (shape: { layerId: RowRefValue; canvasId: RowRefValue }) =>
    allOf([unlockedLayerOnSameCanvas(shape), canEditCanvas(shape.canvasId)]);
  policy.shapes.allowRead.where(allowedTo.read("canvas"));
  policy.shapes.allowInsert.where((shape) => canWriteShape(shape));
  policy.shapes.allowUpdate
    .whereOld((shape) => canWriteShape(shape))
    .whereNew((shape) => canWriteShape(shape));
  policy.shapes.allowDelete.where((shape) => canWriteShape(shape));

  // Asset bytes are immutable once uploaded; a replacement is a new asset.
  policy.assets.allowRead.where(allowedTo.read("canvas"));
  policy.assets.allowInsert.where((asset) => canEditCanvas(asset.canvasId));
  policy.assets.allowUpdate.never();
  policy.assets.allowDelete.where((asset) => canEditCanvas(asset.canvasId));

  // Presence is replaceable ephemera owned by its author. Any member, viewers
  // included, may publish exactly their own cursor on a canvas they belong
  // to; nobody may write or remove another member's cursor.
  policy.cursors.allowRead.where(allowedTo.read("canvas"));
  policy.cursors.allowInsert.where((cursor) =>
    allOf([{ author: session.user.account }, isMemberOf(cursor.canvasId)]),
  );
  policy.cursors.allowUpdate
    .whereOld({ author: session.user.account })
    .whereNew((cursor) => allOf([{ author: session.user.account }, isMemberOf(cursor.canvasId)]));
  policy.cursors.allowDelete.where({ author: session.user.account });

  // Checkpoints are admin-owned, immutable named snapshots, not cursor history.
  policy.checkpoints.allowRead.where(allowedTo.read("canvas"));
  policy.checkpoints.allowInsert.where((checkpoint) => isAdminOf(checkpoint.canvasId));
  policy.checkpoints.allowUpdate.never();
  policy.checkpoints.allowDelete.never();
});
