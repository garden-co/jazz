import { definePermissions } from "../permissions/index.js";
import { schema as s } from "../schema-namespace.js";
import { encryptionSpacePolicySchema } from "./current-membership-schema.js";
import { e2eeAccountRelation } from "./account-relation.js";

const app = s.defineApp({
  ...encryptionSpacePolicySchema("projects"),
  projects: s.table({ name: s.string() }, {}),
  groupOwners: s.table(
    { groupId: s.uuid(), accountId: s.uuid() },
    { group: s.rel("__e2ee_groups", "groupId"), account: e2eeAccountRelation() },
  ),
  notes: s
    .table({ projectId: s.uuid(), body: s.string() }, { project: s.rel("projects", "projectId") })
    .encrypted({ space: "projectId", columns: ["body"] }),
});

function checkGroupPolicies() {
  definePermissions(app, ({ e2ee, session, policy: p }) => {
    e2ee.groups.allowInsert.where({ creatorAccountId: session.user.account });
    e2ee.groups.allowUpdate.where((group) =>
      p.groupOwners.exists.where({ groupId: group.id, accountId: session.user.account }),
    );
    // @ts-expect-error Group roots are immutable; there is no deletion operation.
    e2ee.groups.allowDelete.always();
    // @ts-expect-error The helper does not expose signed-history fields.
    e2ee.groups.allowUpdate.where({ epochId: "forged" });
    // @ts-expect-error Creator account IDs remain strings.
    e2ee.groups.allowInsert.where({ creatorAccountId: 42 });
    e2ee.groups.allowUpdate.where((group) => {
      // @ts-expect-error Callback rows expose only the public group identity.
      return p.projects.exists.where({ id: group.accountId });
    });
    e2ee.groups.allowUpdate.where((group) => {
      // @ts-expect-error Use creatorAccountId as a direct predicate; callbacks correlate the group ID.
      return p.projects.exists.where({ name: group.creatorAccountId });
    });
  });
}
void checkGroupPolicies;
