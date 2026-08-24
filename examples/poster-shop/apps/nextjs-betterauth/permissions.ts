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
  for (const table of [
    policy.layers,
    policy.assets,
    policy.shapes,
    policy.cursors,
    policy.checkpoints,
  ]) {
    table.allowRead.where(allowedTo.read("canvasId"));
    table.allowInsert.where(allowedTo.insert("canvasId"));
    table.allowUpdate.where(allowedTo.update("canvasId"));
    table.allowDelete.where(allowedTo.delete("canvasId"));
  }
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
