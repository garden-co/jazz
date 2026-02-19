---
name: jazz-permissions-security
description: Use this skill when designing data schemas, implementing sharing workflows, or auditing access control in Jazz applications. It covers the hierarchy of Groups, Accounts, and CoValues, ensuring data is private by default and shared securely through cascading permissions and invitations.
---
# Jazz Permissions & Security

## When to Use This Skill

* Structuring apps for multi-tenant or multi-user collaboration
* Implementing sharing workflows, "Share" buttons, Invite Links, or Team management
* Deciding who should own CoValues
* Debugging "User cannot see data" or "Data is read-only" issues
* Configuring permissions for Server Workers. Remember: Workers are just Accounts; they need to be invited to Groups like any other user

## Do NOT Use This Skill For

* Creating or designing schemas (use the `jazz-schema-design` skill)
* Authentication
* Generic UI component styling

## Key Heuristic for Agents

If a user asks "How do I share X with Y?" or "Why can't I access this?", this is usually a Group Ownership issue.

## Core Concepts

Security is **cryptographic** and **group-based**. Every CoValue has an owner, and access is controlled through Groups. You add users to a **Group**, and that Group owns the CoValues.

**Groups can be members of other groups**, creating hierarchical permission structures with inherited roles.

**Critical Rule:** Just because List A contains a reference to Item B does **NOT** mean readers of List A can see Item B. Item B must be owned by a Group the reader has access to.

## The Ownership Hierarchy

### Private

When creating CoValues without a specified owner, Jazz creates a new Group with the current account as the owner and sole admin member.

**Code:** `MyMap.create({ ... })`

### Shared Data

To share data, you can either create a new Group and add members to it, or use an existing Group with multiple members.

**Code:** `MyMap.create({ ... }, { owner: teamGroup })`

## Roles & Permissions Matrix

Jazz uses fixed roles. You cannot create custom roles.

| Role | Capability | Best For |
| ------ | ------------ | ---------- |
| admin | Read, Write, Delete, Invite Members, Revoke Access, Change Roles | Team Owners, Creators |
| manager | Read, Write, Add/Remove readers/writers | Delegated management |
| writer | Read, Write | Collaborators, Team Members |
| reader | Read Only | Observers, Public Links |
| writeOnly | Write Only (Blind submissions) | Voting, Dropboxes |

All users can downgrade themselves or leave a group. Admins cannot be removed/downgraded except by themselves. Managers cannot remove/downgrade each other, but can remove/downgrade lower roles.

**Note:** Only admins can delete CoValues.

## Managing Groups

When assigning roles, you can add *Accounts*, *Groups*, or "everyone". When adding a group, the most permissive role wins for members with multiple entitlements.

```ts
const group = co.group().create();
const bob = await co.account().load(bobsId);

if (bob.$isLoaded) {
  group.addMember(bob, "writer");
  group.addMember(bob, "reader"); // Change role
  group.removeMember(bob);
}
```

## Validating Permissions

```ts
const red = MyCoMap.create({ color: "red" });
const me = co.account().getMe();

if (me.canAdmin(red)) {
  console.log("I can add users of any role");
} else if (me.canManage(red)) {
  console.log("I can share value with others");
} else if (me.canWrite(red)) {
  console.log("I can edit value");
} else if (me.canRead(red)) {
  console.log("I can view value");
}

// Or get role directly
red.$jazz.owner.getRoleOf(me.$jazz.id); // "admin"
```

## Fundamental Patterns

### Pattern 1: Creating Shared Data

**Must** pass the correct owner explicitly to ensure visibility.

```ts
// ❌ WRONG: Defaults to private, other members won't see it
const task = Task.create({ title: "Fix bug" });
project.tasks.push(task);

// ✅ RIGHT: Explicitly set owner
const task = Task.create(
  { title: "Fix bug" },
  { owner: project.$jazz.owner }
);
project.tasks.push(task);

// ✅ ALSO RIGHT: Create new group for independent permissions
const taskGroup = co.group().create();
taskGroup.addMember(project.$jazz.owner, 'writer');
const task = Task.create({ title: "Fix bug" }, { owner: taskGroup });
```

**Note:** Inline creation (passing JSON) automatically handles group inheritance based on schema configuration (default is `extendsContainer`).

You **MUST NOT** use an `Account` as a CoValue owner.

### Pattern 2: The Invite Flow

#### Creating Invite Links

**React:**

```ts
import { createInviteLink } from "jazz-tools/react";
const inviteLink = createInviteLink(organization, "writer");
```

**Svelte:**

```ts
import { createInviteLink } from "jazz-tools/svelte";
const inviteLink = createInviteLink(organization, "writer");
```

Generates URL: `.../#/invite/[CoValue ID]/[inviteSecret]`

#### Accepting Invites

**React:**

```tsx
import { useAcceptInvite } from "jazz-tools/react";

useAcceptInvite({
  invitedObjectSchema: Organization,
  onAccept: async (organizationID) => {
    const organization = await Organization.load(organizationID);
    if (!organization.$isLoaded) throw new Error("Could not load");
    me.root.organizations.$jazz.push(organization);
  },
});
```

**Svelte:**

```svelte
<script lang="ts">
  import { InviteListener } from "jazz-tools/svelte";
  
  new InviteListener({
    invitedObjectSchema: Organization,
    onAccept: async (organizationID) => {
      const organization = await Organization.load(organizationID);
      if (!organization.$isLoaded) throw new Error("Could not load");
      me.current.root.organizations.$jazz.push(organization);
    },
  });
</script>
```

**Programmatic:**

```ts
await account.acceptInvite(organizationId, inviteSecret, Organization);
```

#### Invite Secrets

```ts
const groupToInviteTo = Group.create();
const readerInvite = groupToInviteTo.$jazz.createInvite("reader");
await account.acceptInvite(group.$jazz.id, readerInvite);
```

**⚠️ Security:** Invites do not expire and cannot be revoked. Never pass secrets as route parameters or query strings—only use fragment identifiers (hash in URL).

### Pattern 3: Public Data

"Public" means "readable by anyone who knows the CoValue ID".

```ts
const group = Group.create();
group.addMember("everyone", "writer");
// Or use alias
group.makePublic("writer"); // Defaults to "reader"
```

### Pattern 4: Requesting Invites

Use `writeOnly` role for request lists—users can submit requests but not read others.

```ts
const JoinRequest = co.map({
  account: co.account(),
  status: z.literal(["pending", "approved", "rejected"]),
});

function createRequestsToJoin() {
  const requestsGroup = Group.create();
  requestsGroup.addMember("everyone", "writeOnly");
  return RequestsList.create([], requestsGroup);
}

async function sendJoinRequest(requestsList, account) {
  const request = JoinRequest.create(
    { account, status: "pending" },
    requestsList.$jazz.owner
  );
  requestsList.$jazz.push(request);
}

async function approveJoinRequest(joinRequest, targetGroup) {
  const account = await co.account().load(joinRequest.$jazz.refs.account.id);
  if (account.$isLoaded) {
    targetGroup.addMember(account, "reader");
    joinRequest.$jazz.set("status", "approved");
    return true;
  }
  return false;
}
```

### Pattern 5: Cascading Permissions (Groups as Members)

Groups can be added as members of other groups, creating hierarchies.

```ts
const playlistGroup = Group.create();
const trackGroup = Group.create();
trackGroup.addMember(playlistGroup);
```

When you add groups as members:

* Permissions are granted indirectly
* Roles are inherited (except `writeOnly`)
* Revoking access from member group removes access to container group

**Warning:** Deep nesting can cause performance issues.

#### Role Inheritance Rules

**Most Permissive Role Wins:**

```ts
const addedGroup = Group.create();
addedGroup.addMember(bob, "reader");

const containingGroup = Group.create();
containingGroup.addMember(bob, "writer");
containingGroup.addMember(addedGroup);
// Bob stays writer (higher than inherited reader)
```

#### Overriding Roles

```ts
const organizationGroup = Group.create();
organizationGroup.addMember(bob, "admin");

const billingGroup = Group.create();
billingGroup.addMember(organizationGroup, "reader");
// All org members get reader access to billing, regardless of org role
```

#### Other Operations

```ts
// Remove group
containingGroup.removeMember(addedGroup);

// Get parent groups
containingGroup.getParentGroups(); // [addedGroup]
```

#### Inline CoValue Creation

Jazz automatically manages group ownership for nested CoValues:

```ts
const board = Board.create({
  title: "My board",
  columns: [["Task 1.1", "Task 1.2"], ["Task 2.1", "Task 2.2"]],
});
```

Each column and task gets a new group that inherits from the referencing CoValue's owner.

#### Example: Team Hierarchy

```ts
const companyGroup = Group.create();
companyGroup.addMember(CEO, "admin");

const teamGroup = Group.create();
teamGroup.addMember(companyGroup);
teamGroup.addMember(teamLead, "admin");
teamGroup.addMember(developer, "writer");

const projectGroup = Group.create();
projectGroup.addMember(teamGroup);
projectGroup.addMember(client, "reader");
```

## Troubleshooting

### "User cannot see data"
1. **Verify Ownership**: Is the CoValue owned by the expected Group? Check `$jazz.owner`.
2. **Verify Membership**: Is the target user a member of that Group?
3. **Check References**: Does the user have a way to discover the ID (e.g. through a reference in their account root)?

### "Data is read-only"
1. **Check Role**: Use `red.$jazz.owner.getRoleOf(me.$jazz.id)` to check the actual role. `reader` cannot write.

### Cascading Issues
1. **Group Membership**: If Group A is a member of Group B, check that the user is a member of Group A with a role that permits the desired action in Group B.
2. **Unsupported Roles**: Remember `writeOnly` does not cascade.

## Quick Reference

**Ownership:** Admin/manager modifies group membership, not CoValue ownership, which cannot be modified.

**Permission inheritance:** Nested CoValues inherit permissions from parent when created inline (behavior can be modified at the schema level).

**Access control:** Only members of a Group can access CoValues owned by that Group. References alone don't grant access.

**Public access:** `makePublic()` or `addMember("everyone", "reader")` makes Groups readable by anyone with the Group ID.

**Invite links:** `createInviteLink()` generates shareable URLs. Accept with `useAcceptInvite()` (React), `InviteListener` (Svelte) or `account.acceptInvite()`.

**Invite security:** Never pass secrets as route parameters/query strings. Only use fragment identifiers.

**Requesting access:** `writeOnly` role on requests list allows non-members to submit join requests for admin review.

**Cascading:** Groups can be members of other groups. Roles inherit (admin, manager, writer, reader), but `writeOnly` doesn't. Most permissive role wins. Use `getParentGroups()` to inspect hierarchy.

## References

* Permissions Overview: <https://jazz.tools/docs/permissions-and-sharing/overview.md>
* Invite Links: <https://jazz.tools/docs/permissions-and-sharing/sharing.md>
* Cascading Rules: <https://jazz.tools/docs/permissions-and-sharing/cascading-permissions.md>
* Chat Example: <https://github.com/garden-co/jazz/tree/main/examples/chat>
* Todo Example: <https://github.com/garden-co/jazz/tree/main/examples/todo>

When using an online reference via a skill, cite the specific URL to the user to build trust.
