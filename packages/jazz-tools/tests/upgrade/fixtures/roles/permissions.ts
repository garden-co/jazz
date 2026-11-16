import { schema as s } from "jazz-tools";
import { app } from "./schema.js";
export default s.definePermissions(app, ({ policy, session, anyOf, allOf, allowedTo }) => {
  const me = session.identity.subject;
  const role = (orgId: any, ...roles: ("admin" | "editor" | "viewer")[]) =>
    policy.members.exists.where({ orgId, userId: me, role: { in: roles } });
  policy.orgs.allowRead.where((o) =>
    anyOf([role(o.id, "admin", "editor", "viewer"), session.where({ "claims.staff": true })]),
  );
  policy.orgs.allowInsert.never();
  policy.orgs.allowUpdate.where((o) => role(o.id, "admin"));
  policy.orgs.allowDelete.never();
  policy.members.allowRead.where((m) => role(m.orgId, "admin", "editor", "viewer"));
  policy.members.allowInsert.where((m) => allOf([role(m.orgId, "admin"), { userId: { ne: me } }]));
  policy.members.allowUpdate
    .whereOld((m) => allOf([role(m.orgId, "admin"), { userId: { ne: me } }]))
    .whereNew((m) => policy.members.exists.where({ id: m.id, orgId: m.orgId, userId: m.userId }));
  policy.members.allowDelete.where((m) => role(m.orgId, "admin"));
  // docs: org members by role, staff claim, or explicit share; hopTo through doc->org
  policy.docs.allowRead.where((d) =>
    anyOf([
      role(d.orgId, "admin", "editor"),
      allOf([role(d.orgId, "viewer"), { status: "published" }]),
      session.where({ "claims.staff": true }),
      policy.docShares.exists.where({ docId: d.id, userId: me }),
    ]),
  );
  policy.docs.allowInsert.where((d) => allOf([role(d.orgId, "admin", "editor"), { authorId: me }]));
  policy.docs.allowUpdate
    .whereOld((d) => allOf([role(d.orgId, "admin", "editor"), { status: { ne: "locked" } }]))
    .whereNew((d) =>
      allOf([
        role(d.orgId, "admin", "editor"),
        policy.docs.exists.where({ id: d.id, orgId: d.orgId, authorId: d.authorId }),
      ]),
    );
  policy.docs.allowDelete.where((d) => role(d.orgId, "admin"));
  policy.docShares.allowRead.where((sh) =>
    anyOf([
      { userId: me },
      policy.exists(policy.docShares.where({ id: sh.id }).hopTo("doc").where({ authorId: me })),
    ]),
  );
  policy.docShares.allowInsert.where((sh) =>
    policy.docs.exists.where({ id: sh.docId, authorId: me }),
  );
  policy.docShares.allowUpdate.never();
  policy.docShares.allowDelete.where(allowedTo.update("doc"));
  // comments inherit: read if can read the doc, insert if can read it
  policy.comments.allowRead.where(allowedTo.read("doc"));
  policy.comments.allowInsert.where(allowedTo.read("doc"));
  policy.comments.allowUpdate.never();
  policy.comments.allowDelete.where(allowedTo.update("doc"));
});
