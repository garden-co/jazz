import { co, Group, z } from "jazz-tools";
// #region Basic
const playlistGroup = co.group().create();
const trackGroup = co.group().create();

// Tracks are now visible to the members of playlist
trackGroup.addMember(playlistGroup);
// #endregion

// #region Inheritance
const grandParentGroup = co.group().create();
const parentGroup = co.group().create();
const childGroup = co.group().create();

childGroup.addMember(parentGroup);
parentGroup.addMember(grandParentGroup);
//#endregion

import { createJazzTestAccount } from "jazz-tools/testing";
const bob = await createJazzTestAccount();

// #region RuleOfMostPermissive
const addedGroup = co.group().create();
addedGroup.addMember(bob, "reader");

const containingGroup = co.group().create();
addedGroup.addMember(bob, "writer");
containingGroup.addMember(addedGroup);

// Bob stays a writer because his role is higher
// than the inherited reader role.
// #endregion
{
  // #region WriteOnlyOmitted

  const addedGroup = co.group().create();
  containingGroup.addMember(bob, "writeOnly");

  const mainGroup = co.group().create();
  mainGroup.addMember(containingGroup);
  // #endregion
}

// #region Overrides
const organizationGroup = co.group().create();
organizationGroup.addMember(bob, "admin");

const billingGroup = co.group().create();

// This way the members of the organization
// can only read the billing data
billingGroup.addMember(organizationGroup, "reader");
// #endregion

const alice = await createJazzTestAccount();
{
  // #region OverrideContainers
  const addedGroup = co.group().create();
  addedGroup.addMember(alice, "admin");
  addedGroup.addMember(bob, "reader");

  const containingGroup = co.group().create();
  containingGroup.addMember(addedGroup, "writer");
}

// Bob and Alice are now writers in the containing group

// #region RemoveMembers
// Remove member from added group
addedGroup.removeMember(bob);

// Bob loses access to both groups.
// If Bob was also a member of the containing group,
// he wouldn't have lost access.
// #endregion

{
  // #region RevokeExtension
  const addedGroup = co.group().create();
  const containingGroup = co.group().create();

  containingGroup.addMember(addedGroup);

  // Revoke the extension
  containingGroup.removeMember(addedGroup);
  // #endregion
}
{
  // #region GetParentGroups
  const containingGroup = co.group().create();
  const addedGroup = co.group().create();
  containingGroup.addMember(addedGroup);

  console.log(containingGroup.getParentGroups()); // [addedGroup]
  // #endregion
}

// #region ImplicitOwnership
const Task = co.plainText();
const Column = co.list(Task);
const Board = co.map({
  title: z.string(),
  columns: co.list(Column),
});

const board = Board.create({
  title: "My board",
  columns: [
    ["Task 1.1", "Task 1.2"],
    ["Task 2.1", "Task 2.2"],
  ],
});
// #endregion

// #region ManageImplicitPermissions
// @ts-expect-error Redeclaring
const writeAccess = co.group().create();
writeAccess.addMember(bob, "writer");

// Give Bob write access to the board, columns and tasks
const boardWithGranularPermissions = Board.create(
  {
    title: "My board",
    columns: [
      ["Task 1.1", "Task 1.2"],
      ["Task 2.1", "Task 2.2"],
    ],
  },
  writeAccess,
);

// Give Alice read access to one specific task
const task = boardWithGranularPermissions.columns[0][0];
const taskGroup = task.$jazz.owner;
taskGroup.addMember(alice, "reader");
// #endregion

// #region ExplicitPermissions
// @ts-expect-error Redeclaring
const writeAccess = co.group().create();
writeAccess.addMember(bob, "writer");
const readAccess = co.group().create();
readAccess.addMember(bob, "reader");

// Give Bob read access to the board and write access to the columns and tasks
const boardWithExplicitPermissions = Board.create(
  {
    title: "My board",
    columns: co.list(Column).create(
      [
        ["Task 1.1", "Task 1.2"],
        ["Task 2.1", "Task 2.2"],
      ],
      writeAccess,
    ),
  },
  readAccess,
);
// #endregion

const CEO = await createJazzTestAccount();
const teamLead = await createJazzTestAccount();
const developer = await createJazzTestAccount();
const client = await createJazzTestAccount();
// #region TeamHierarchy
// Company-wide group
const companyGroup = co.group().create();
companyGroup.addMember(CEO, "admin");

// Team group with elevated permissions
const teamGroup = co.group().create();
teamGroup.addMember(companyGroup); // Inherits company-wide access
teamGroup.addMember(teamLead, "admin");
teamGroup.addMember(developer, "writer");

// Project group with specific permissions
const projectGroup = co.group().create();
projectGroup.addMember(teamGroup); // Inherits team permissions
projectGroup.addMember(client, "reader"); // Client can only read project items
// #endregion
