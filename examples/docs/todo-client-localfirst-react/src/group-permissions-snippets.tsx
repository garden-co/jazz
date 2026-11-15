import { useAll, useDb } from "jazz-tools/react";
import { app } from "./recipes/group-permissions/jazz.generated.js";

// #region group-create
export async function createWorkspace(
  db: ReturnType<typeof useDb>,
  name: string,
  creatorId: string,
) {
  const { value: workspace } = await db.insert(app.workspaces, { name });
  // Add the creator as admin immediately so they can manage the workspace
  db.insert(app.workspaceMembers, {
    workspaceId: workspace.id,
    user_id: creatorId,
    role: "admin",
  });
  return workspace;
}
// #endregion group-create

// #region group-add-member
export async function addMember(
  db: ReturnType<typeof useDb>,
  workspaceId: string,
  userId: string,
  role: "reader" | "writer" | "contributor" | "admin",
) {
  await db.insert(app.workspaceMembers, { workspaceId, user_id: userId, role });
}
// #endregion group-add-member

// #region group-query-docs
export function WorkspaceDocuments({ workspaceId }: { workspaceId: string }) {
  const { data: docs, isLoading, error } = useAll(app.documents.where({ workspaceId }));

  if (isLoading) return <p>Loading…</p>;
  if (error) return <p>Something went wrong!</p>;

  return (
    <ul>
      {docs.map((doc) => (
        <li key={doc.id}>{doc.title}</li>
      ))}
    </ul>
  );
}
// #endregion group-query-docs

// #region group-members-list
export function WorkspaceMembers({ workspaceId }: { workspaceId: string }) {
  const { data: members = [], isLoading } = useAll(app.workspaceMembers.where({ workspaceId }));

  if (isLoading) return <p>Loading…</p>;

  return (
    <ul>
      {members.map((member) => (
        <li key={member.id}>
          {member.user_id} — {member.role}
        </li>
      ))}
    </ul>
  );
}
// #endregion group-members-list

// #region group-change-role
export async function changeRole(
  db: ReturnType<typeof useDb>,
  memberId: string,
  newRole: "reader" | "contributor" | "writer" | "admin",
) {
  await db.update(app.workspaceMembers, memberId, { role: newRole });
}
// #endregion group-change-role

// #region group-remove-member
export async function removeMember(
  db: ReturnType<typeof useDb>,
  workspaceId: string,
  userId: string,
) {
  const member = await db.one(app.workspaceMembers.where({ workspaceId, user_id: userId }));
  if (member) await db.delete(app.workspaceMembers, member.id);
}
// #endregion group-remove-member
