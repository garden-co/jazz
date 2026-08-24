import { definePermissions, type RowContext } from "jazz-tools/permissions";
import { app, type Canvas } from "./schema.js";

type Role = "viewer" | "editor" | "admin";

export default definePermissions(app, ({ policy, session, anyOf, allOf, allowedTo }) => {
  const hasRole = (canvas: RowContext<Canvas>, role: Role) =>
    policy.canvasMembers.exists.where({ canvasId: canvas.id, userId: session.user_id, role });
  const canRead = (canvas: RowContext<Canvas>) =>
    policy.canvasMembers.exists.where({ canvasId: canvas.id, userId: session.user_id });
  const isAdmin = (canvas: RowContext<Canvas>) => hasRole(canvas, "admin");

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
        policy.canvases.exists.where({ id: member.canvasId, $createdBy: session.user_id }),
      ]),
    ]),
  );
  policy.canvasMembers.allowUpdate.where(allowedTo.update("canvasId"));
  policy.canvasMembers.allowDelete.where(
    anyOf([allowedTo.update("canvasId"), { userId: session.user_id }]),
  );
  policy.layers.allowRead.where(allowedTo.read("canvasId"));
  policy.layers.allowInsert.where(allowedTo.insert("canvasId"));
  policy.layers.allowUpdate.where(allowedTo.update("canvasId"));
  policy.layers.allowDelete.where(allowedTo.delete("canvasId"));
  policy.assets.allowRead.where(allowedTo.read("canvasId"));
  policy.assets.allowInsert.where(allowedTo.insert("canvasId"));
  policy.assets.allowUpdate.where(allowedTo.update("canvasId"));
  policy.assets.allowDelete.where(allowedTo.delete("canvasId"));
  policy.shapes.allowRead.where(allowedTo.read("canvasId"));
  policy.shapes.allowInsert.where(allowedTo.insert("canvasId"));
  policy.shapes.allowUpdate.where(allowedTo.update("canvasId"));
  policy.shapes.allowDelete.where(allowedTo.delete("canvasId"));
  policy.cursors.allowRead.where(allowedTo.read("canvasId"));
  policy.cursors.allowInsert.where(allowedTo.insert("canvasId"));
  policy.cursors.allowUpdate.where(allowedTo.update("canvasId"));
  policy.cursors.allowDelete.where(allowedTo.delete("canvasId"));
  policy.checkpoints.allowRead.where(allowedTo.read("canvasId"));
  policy.checkpoints.allowInsert.where(allowedTo.insert("canvasId"));
  policy.checkpoints.allowUpdate.where(allowedTo.update("canvasId"));
  policy.checkpoints.allowDelete.where(allowedTo.delete("canvasId"));
  // Presence is replaceable ephemera: only its owner may update/delete it.
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
  policy.checkpoints.allowDelete.where(allowedTo.update("canvasId"));
});
