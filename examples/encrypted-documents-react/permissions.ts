import { definePermissions, RowContext } from "jazz-tools/permissions";
import { app, Organization } from "./schema.js";

export default definePermissions(app, ({ policy, session, allowedTo, anyOf }) => {
  const isOrgMember = (org: RowContext<Organization>) =>
    policy.members.exists.where({ orgId: org.id, userId: session.user_id });

  policy.organizations.allowRead.where((org) =>
    anyOf([{ createdBy: session.user_id }, isOrgMember(org)]),
  );
  policy.organizations.allowInsert.where({ createdBy: session.user_id });
  policy.organizations.allowUpdate.whereOld(isOrgMember).whereNew(isOrgMember);
  policy.organizations.allowDelete.where({ createdBy: session.user_id });

  policy.members.allowRead.where(allowedTo.read("orgId"));
  policy.members.allowInsert.where({ userId: session.user_id });
  policy.members.allowUpdate.never();
  policy.members.allowDelete.where({ userId: session.user_id });

  policy.documents.allowRead.where(allowedTo.read("orgId"));
  policy.documents.allowInsert.where(allowedTo.read("orgId"));
  policy.documents.allowUpdate.never();
  policy.documents.allowDelete.where(allowedTo.read("orgId"));

  policy.document_parts.allowRead.where(allowedTo.read("orgId"));
  policy.document_parts.allowInsert.where(allowedTo.read("orgId"));
  policy.document_parts.allowUpdate.never();
  policy.document_parts.allowDelete.where(allowedTo.read("orgId"));
});
