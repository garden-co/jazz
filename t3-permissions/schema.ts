import { schema as s } from "jazz-tools";
export const schema = s.defineSchema({
  orgs: s.table({ name: s.string() }, { members: s.reverse("members", "org"), docs: s.reverse("docs", "org") }),
  members: s.table({ orgId: s.uuid(), userId: s.string(), role: s.enum("admin", "editor", "viewer") }, { org: s.rel("orgs", "orgId") }),
  docs: s.table({ orgId: s.uuid(), title: s.string(), status: s.enum("draft", "published", "locked"), authorId: s.string() }, { org: s.rel("orgs", "orgId"), comments: s.reverse("comments", "doc"), shares: s.reverse("docShares", "doc") }),
  docShares: s.table({ docId: s.uuid(), userId: s.string() }, { doc: s.rel("docs", "docId") }),
  comments: s.table({ docId: s.uuid(), body: s.string() }, { doc: s.rel("docs", "docId") }),
});

const { definePermissions, exists, allowedTo, session, anyOf, allOf, eq, ne } = schema;

const authorIdFromSession = { authorId: session.identity.subject };

const rule1 = (orgId: s.Uuid) =>
  exists.members({ orgId, userId: session.identity.subject, role: ["admin", "editor"] });

const isMemberWithRole = (orgId: s.Uuid, role: "admin" | "editor" | "viewer") =>
  exists.members({ orgId, userId: session.identity.subject, role: [role] });

const staffIsTrue = eq(session.claims.staff, true);

const isMember = (orgId: s.Uuid) =>
  exists.members({ orgId, userId: session.identity.subject, role: ["admin", "editor", "viewer"] });

const rule2 = (orgId: s.Uuid) =>
  allOf(isMemberWithRole(orgId, "admin"), { userId: ne(session.identity.subject) });

export const permissions = definePermissions({
  comments: {
    read: (comment) => allowedTo.read(comment.doc),
    insert: (comment) => allowedTo.read(comment.doc),
    update: false,
    delete: (comment) => allowedTo.update(comment.doc),
  },
  docShares: {
    read: (docShare) =>
      anyOf(
        { userId: session.identity.subject },
        exists.docShares({ id: docShare.id }).hop("doc", authorIdFromSession),
      ),
    insert: (docShare) => docShare.doc.where(authorIdFromSession),
    update: false,
    delete: (docShare) => allowedTo.update(docShare.doc),
  },
  docs: {
    read: (doc) =>
      anyOf(
        rule1(doc.orgId),
        allOf(isMemberWithRole(doc.orgId, "viewer"), { status: "published" }),
        staffIsTrue,
        exists.docShares({ docId: doc.id, userId: session.identity.subject }),
      ),
    insert: (doc) => allOf(rule1(doc.orgId), authorIdFromSession),
    updateIf: (doc) => allOf(rule1(doc.orgId), { status: ne("locked") }),
    updateTo: (doc, old) => allOf(rule1(doc.orgId), { orgId: old.orgId, authorId: old.authorId }),
    delete: (doc) => isMemberWithRole(doc.orgId, "admin"),
  },
  members: {
    read: (member) => isMember(member.orgId),
    insert: (member) => rule2(member.orgId),
    updateIf: (member) => rule2(member.orgId),
    updateTo: (member, old) => ({ orgId: old.orgId, userId: old.userId }),
    delete: (member) => isMemberWithRole(member.orgId, "admin"),
  },
  orgs: {
    read: (org) => anyOf(isMember(org.id), staffIsTrue),
    insert: false,
    update: (org) => isMemberWithRole(org.id, "admin"),
    delete: false,
  },
});
