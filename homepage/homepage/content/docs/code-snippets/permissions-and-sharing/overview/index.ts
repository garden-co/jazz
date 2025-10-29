// #region Basic
import { Group } from "jazz-tools";

const group = Group.create();
// #endregion

const bobsId = "";
// #region AddMember
import { co } from "jazz-tools";
const bob = await co.account().load(bobsId);

if (bob.$isLoaded) {
  group.addMember(bob, "writer");
}
// #endregion

// #region ChangeRole
if (bob.$isLoaded) {
  group.addMember(bob, "reader");
}
// #endregion

// #region RemoveMember
if (bob.$isLoaded) {
  group.removeMember(bob);
}
// #endregion

import { createJazzTestAccount } from "jazz-tools/testing";
import { z } from "jazz-tools";
const existingCoValue = await createJazzTestAccount();

const MyCoMap = co.map({
  color: z.string(),
});

// #region GetGroup
const owningGroup = existingCoValue.$jazz.owner;
const newValue = MyCoMap.create({ color: "red" }, { owner: group });
// #endregion

// #region CheckPermissions
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
} else {
  console.log("I cannot access value");
}
// #endregion

const alicesId = "";

// #region CheckPermissionsAlice
const blue = MyCoMap.create({ color: "blue" });
const alice = await co.account().load(alicesId);

if (alice.$isLoaded) {
  if (alice.canAdmin(blue)) {
    console.log("Alice can share value with others");
  } else if (alice.canWrite(blue)) {
    console.log("Alice can edit value");
  } else if (alice.canRead(blue)) {
    console.log("Alice can view value");
  } else {
    console.log("Alice cannot access value");
  }
}
// #endregion
