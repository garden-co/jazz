import { Account, Group, co } from "jazz-tools";
// #region Basic
const group = Group.create();
group.addMember("everyone", "writer");
// #endregion

{
  // #region MakePublic
  const group = Group.create();
  group.addMember("everyone", "writer"); // [!code --]
  group.makePublic("writer"); // [!code ++]
  // group.makePublic(); // Defaults to "reader" access
  // #endregion
}

import { Organization } from "./schema";
const organization = Organization.create({ name: "Garden Computing" });
const organizationId = organization.$jazz.id;
const inviteSecret = "inviteSecret_z";
const account = Account.getMe();
// #region AcceptInviteProgrammatically
await account.acceptInvite(organizationId, inviteSecret, Organization);
// #endregion

// #region InviteToGroup
const groupToInviteTo = Group.create();
const readerInvite = groupToInviteTo.$jazz.createInvite("reader");
// `inviteSecret_`

await account.acceptInvite(group.$jazz.id, readerInvite);
// #endregion

import { RequestsList, JoinRequest } from "./schema";
// #region CreateRequestsToJoin
function createRequestsToJoin() {
  const requestsGroup = Group.create();
  requestsGroup.addMember("everyone", "writeOnly");

  return RequestsList.create([], requestsGroup);
}

async function sendJoinRequest(
  requestsList: co.loaded<typeof RequestsList>,
  account: Account,
) {
  const request = JoinRequest.create(
    {
      account,
      status: "pending",
    },
    requestsList.$jazz.owner, // Inherit the access controls of the requestsList
  );

  requestsList.$jazz.push(request);

  return request;
}
// #endregion

// #region ApproveJoinRequest
async function approveJoinRequest(
  joinRequest: co.loaded<typeof JoinRequest, { account: true }>,
  targetGroup: Group,
) {
  const account = await co.account().load(joinRequest.$jazz.refs.account.id);

  if (account.$isLoaded) {
    targetGroup.addMember(account, "reader");
    joinRequest.$jazz.set("status", "approved");

    return true;
  } else {
    return false;
  }
}
// #endregion
