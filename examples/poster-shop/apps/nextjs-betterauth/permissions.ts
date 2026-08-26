import { definePermissions, type RowContext } from "jazz-tools/permissions";
import { app, type Canvas } from "./schema";

type Role = "viewer" | "editor" | "admin";

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
    policy.canvasMembers.exists.where({ canvasId: canvas.id, userId: session.user_id, role });
  const canRead = (canvas: RowContext<Canvas>) =>
    policy.canvasMembers.exists.where({ canvasId: canvas.id, userId: session.user_id });
  const isAdmin = (canvas: RowContext<Canvas>) => hasRole(canvas, "admin");
  // `allowedTo.insert` composes the parent's insert rule. Canvas bootstrap is
  // intentionally unconditional, so child insert rules must spell out their
  // own membership predicate instead.

  policy.canvases.allowRead.where((canvas) => canRead(canvas));
  policy.canvases.allowInsert.always();
  policy.canvases.allowUpdate.where((canvas) => isAdmin(canvas));
  policy.canvases.allowDelete.where((canvas) => isAdmin(canvas));
  policy.canvasMembers.allowRead.where(allowedTo.read("canvasId"));
  policy.canvasMembers.allowInsert.where((member) =>
    anyOf([
      allowedTo.update("canvasId"),
      allOf([
        { userId: session.user_id, role: "admin" },
        policy.canvases.exists.where({ id: member.canvasId, $createdBy: session.author }),
      ]),
    ]),
  );
  policy.canvasMembers.allowUpdate.where(allowedTo.update("canvasId"));
  policy.canvasMembers.allowDelete.where(
    anyOf([allowedTo.update("canvasId"), { userId: session.user_id }]),
  );
  policy.layers.allowRead.where(allowedTo.read("canvasId"));
  policy.layers.allowInsert.where((layer) =>
    anyOf([
      policy.canvasMembers.exists.where({
        canvasId: layer.canvasId,
        userId: session.user_id,
        role: "editor",
      }),
      policy.canvasMembers.exists.where({
        canvasId: layer.canvasId,
        userId: session.user_id,
        role: "admin",
      }),
    ]),
  );
  policy.assets.allowRead.where(allowedTo.read("canvasId"));
  policy.shapes.allowRead.where(allowedTo.read("canvasId"));
  // Shape admission has two independently correlated proofs over the row
  // being inserted: the selected layer belongs to this shape's canvas, and
  // the current user is an editor or admin of that same canvas.  Keeping both
  // predicates explicit prevents an editor on one canvas from attaching a
  // shape to a layer on another.
  policy.shapes.allowInsert.where((shape) =>
    allOf([
      policy.layers.exists.where({ id: shape.layerId, canvasId: shape.canvasId }),
      policy.canvasMembers.exists.where({
        canvasId: shape.canvasId,
        userId: session.user_id,
        role: { in: ["editor", "admin"] },
      }),
    ]),
  );
  policy.cursors.allowRead.where(allowedTo.read("canvasId"));
  policy.checkpoints.allowRead.where(allowedTo.read("canvasId"));
  // Presence is replaceable ephemera: only its owner may update/delete it.
  // Creation remains default-deny until its product ownership rule is chosen.
  policy.cursors.allowUpdate
    .whereOld({ userId: session.user_id })
    .whereNew({ userId: session.user_id });
  policy.cursors.allowDelete.where({ userId: session.user_id });
  // Checkpoints are admin-owned durable branch markers, not cursor history.
  policy.checkpoints.allowInsert.where((checkpoint) =>
    policy.canvasMembers.exists.where({
      canvasId: checkpoint.canvasId,
      userId: session.user_id,
      role: "admin",
    }),
  );
  policy.checkpoints.allowUpdate.never();

  // Asset mutation, layer/shape update/delete, cursor creation, and checkpoint
  // deletion remain default-deny. Their ownership rules are tracked in #1926.
});
