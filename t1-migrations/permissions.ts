import { schema as s } from "jazz-tools";
import { app } from "./schema.js";
export default s.definePermissions(app, ({ policy, session, anyOf }) => {
  const canSee = (projectId: any) => anyOf([policy.projects.exists.where({ id: projectId, owner_id: session.user }), policy.projectMembers.exists.where({ projectId, user_id: session.user })]);
  policy.projects.allowRead.where((p) => canSee(p.id));
  policy.projects.allowInsert.where({ owner_id: session.user });
  policy.projects.allowUpdate.where({ owner_id: session.user }); policy.projects.allowDelete.never();
  policy.projectMembers.allowRead.where((m) => canSee(m.projectId));
  policy.projectMembers.allowInsert.where((m) => policy.projects.exists.where({ id: m.projectId, owner_id: session.user }));
  policy.projectMembers.allowUpdate.never(); policy.projectMembers.allowDelete.never();
  policy.tasks.allowRead.where((t) => canSee(t.projectId));
  policy.tasks.allowInsert.where((t) => canSee(t.projectId));
  policy.tasks.allowUpdate.where((t) => canSee(t.projectId)); policy.tasks.allowDelete.never();
});
