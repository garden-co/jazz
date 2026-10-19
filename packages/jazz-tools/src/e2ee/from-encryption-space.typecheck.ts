import { schema as s } from "../schema-namespace.js";
import { definePermissions } from "../permissions/index.js";
import { encryptionSpacePolicySchema } from "./current-membership-schema.js";

const app = s.defineApp({
  ...encryptionSpacePolicySchema("projects"),
  projects: s.table({ title: s.string() }, {}),
  notes: s
    .table(
      { projectId: s.uuid(), body: s.string() },
      {
        project: s.rel("projects", "projectId"),
      },
    )
    .encrypted({ space: "projectId", columns: ["body"] }),
});

// Compile-only checks: each predicate retains the table's normal where and row types.
function checkPredicates() {
  definePermissions(app, ({ policy: p, session, anyOf }) => {
    p.notes.fromEncryptionSpace({
      read: anyOf([{ "$createdBy.account": session.user.account }]),
      insert: { projectId: "11111111-1111-4111-8111-111111111111" },
      update: (note) => p.projects.exists.where({ id: note.projectId }),
      delete: { "$createdBy.account": session.user.account },
      write: { "$createdBy.account": session.user.account },
    });
    // @ts-expect-error Only CRUD operations and the common write predicate are supported.
    p.notes.fromEncryptionSpace({ share: {} });
    // @ts-expect-error A stored column must exist.
    p.notes.fromEncryptionSpace({ read: { missing: "value" } });
    // @ts-expect-error Column values retain their schema types.
    p.notes.fromEncryptionSpace({ insert: { body: 42 } });
    p.notes.fromEncryptionSpace({
      update: (note) => {
        // @ts-expect-error Callback row references only expose declared row fields.
        return p.projects.exists.where({ id: note.missing });
      },
    });
  });
}
void checkPredicates;
