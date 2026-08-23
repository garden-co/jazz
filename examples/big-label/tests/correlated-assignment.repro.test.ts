import { afterEach, describe, expect, it } from "vitest";
import { schema as s } from "jazz-tools";
import { definePermissions } from "jazz-tools/permissions";
import { createPolicyTestApp, type PolicyTestApp } from "jazz-tools/testing";

// Minimal deployed-policy reproduction for CB-012. The foreign membership
// must be denied; current core instead faults while lowering the correlated
// Exists checks used to enforce the tenant boundary.
const app = s.defineApp({
  organizations: s.table({ name: s.string() }),
  memberships: s.table({ organizationId: s.ref("organizations"), userId: s.string() }),
  releases: s.table({ organizationId: s.ref("organizations"), title: s.string() }),
  assignments: s.table({
    organizationId: s.ref("organizations"),
    releaseId: s.ref("releases"),
    membershipId: s.ref("memberships"),
  }),
});
const permissions = definePermissions(app, ({ policy, allOf, allowedTo }) => {
  policy.organizations.allowRead.where({});
  policy.organizations.allowInsert.where({});
  policy.memberships.allowRead.where({});
  policy.memberships.allowInsert.where({});
  policy.releases.allowRead.where({});
  policy.releases.allowInsert.where({});
  policy.assignments.allowRead.where(allowedTo.read("releaseId"));
  policy.assignments.allowInsert.where((row) =>
    allOf([
      allowedTo.insert("releaseId"),
      policy.releases.exists.where({ id: row.releaseId, organizationId: row.organizationId }),
      policy.memberships.exists.where({ id: row.membershipId, organizationId: row.organizationId }),
    ]),
  );
});

let testApp: PolicyTestApp | undefined;
afterEach(async () => await testApp?.shutdown());

describe("CB-012 correlated assignment policy", () => {
  it("rejects a cross-tenant membership instead of faulting the authority", async () => {
    testApp = await createPolicyTestApp(app, permissions, expect);
    const owned = await testApp.seed((db) => db.insert(app.organizations, { name: "owned" }));
    const foreign = await testApp.seed((db) => db.insert(app.organizations, { name: "foreign" }));
    const release = await testApp.seed((db) =>
      db.insert(app.releases, { organizationId: owned.id, title: "owned release" }),
    );
    const membership = await testApp.seed((db) =>
      db.insert(app.memberships, { organizationId: foreign.id, userId: "foreign" }),
    );
    const user = testApp.as({ user_id: "user", claims: {}, authMode: "external" });
    await user.expectDenied((db) =>
      db.insert(app.assignments, {
        organizationId: owned.id,
        releaseId: release.id,
        membershipId: membership.id,
      }),
    );
  });
});
