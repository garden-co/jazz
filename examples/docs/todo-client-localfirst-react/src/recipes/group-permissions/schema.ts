// #region schema
import { schema as s } from "jazz-tools";

export const schema = s.defineSchema({
  documents: s.table(
    { title: s.string(), content: s.string(), workspaceId: s.uuid() },
    {
      workspace: s.rel("workspaces", "workspaceId"),
    },
  ),
  workspaceMembers: s.table(
    {
      workspaceId: s.uuid(),
      user_id: s.uuid(),
      role: s.enum("reader", "writer", "contributor", "admin"),
    },
    {
      workspace: s.rel("workspaces", "workspaceId"),
    },
  ),
  workspaces: s.table(
    { name: s.string() },
    {
      documents: s.reverse("documents", "workspace"),
      members: s.reverse("workspaceMembers", "workspace"),
    },
  ),
});
// #endregion schema

// #region permissions
const { definePermissions, exists, session, anyOf, allOf } = schema;

const isMember = (workspaceId: s.Uuid) =>
  exists.workspaceMembers({ workspaceId, user_id: session.user });

const hasRole = (workspaceId: s.Uuid, role: "reader" | "writer" | "contributor" | "admin") =>
  exists.workspaceMembers({ workspaceId, user_id: session.user, role });

const isAdmin = (workspaceId: s.Uuid) => hasRole(workspaceId, "admin");

const createdByMe = { $createdBy: session.user };

const mine = { user_id: session.user };

export const permissions = definePermissions({
  documents: {
    read: (document) => isMember(document.workspaceId),
    insert: (document) =>
      anyOf(
        hasRole(document.workspaceId, "writer"),
        hasRole(document.workspaceId, "contributor"),
        isAdmin(document.workspaceId),
      ),
    update: (document) =>
      anyOf(
        hasRole(document.workspaceId, "writer"),
        isAdmin(document.workspaceId),
        allOf(createdByMe, hasRole(document.workspaceId, "contributor")),
      ),
    delete: (document) =>
      anyOf(
        hasRole(document.workspaceId, "writer"),
        isAdmin(document.workspaceId),
        allOf(createdByMe, hasRole(document.workspaceId, "contributor")),
      ),
  },
  workspaceMembers: {
    read: (workspaceMember) => isMember(workspaceMember.workspaceId),
    insert: (workspaceMember) =>
      anyOf(
        isAdmin(workspaceMember.workspaceId),
        allOf(
          { user_id: session.user, role: "admin" },
          workspaceMember.workspace.where(createdByMe),
        ),
      ),
    update: (workspaceMember) => isAdmin(workspaceMember.workspaceId),
    delete: (workspaceMember) => anyOf(isAdmin(workspaceMember.workspaceId), mine),
  },
  workspaces: {
    read: (workspace) => isMember(workspace.id),
    insert: true,
    update: (workspace) => isAdmin(workspace.id),
    delete: (workspace) => isAdmin(workspace.id),
  },
});
// #endregion permissions
