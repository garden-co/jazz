import { Account, Group, ID } from "jazz-tools";
import { useCoState } from "../main.tsx";
import {
  JoinOrganizationRequest,
  ListOfJoinOrganizationRequests,
  Organization,
  OrganizationContent,
  acceptJoinOrganizationRequest,
} from "../schema.ts";

export function OrganizationMembers({
  organization,
}: { organization: Organization }) {
  const privateContent = useCoState(
    OrganizationContent,
    organization._refs.content.id,
    {},
  );
  const joinRequests = useCoState(
    ListOfJoinOrganizationRequests,
    organization._refs.joinRequests.id,
    [{}],
  );

  if (!privateContent) return null;

  const group = privateContent._owner.castAs(Group);
  const myRole = group.myRole();

  return (
    <>
      {group.members.map((member) => (
        <Member
          key={member.id}
          accountId={member.id as ID<Account>}
          role={member.role}
        />
      ))}
      {myRole === "admin" &&
        joinRequests?.map((request) => (
          <JoinRequest key={request.id} request={request} />
        ))}
    </>
  );
}
function Member({
  accountId,
  role,
}: { accountId: ID<Account>; role?: string }) {
  const account = useCoState(Account, accountId, { profile: {} });

  if (!account?.profile) return;

  return (
    <div className="px-4 py-5 sm:px-6">
      <strong className="font-medium">{account.profile.name}</strong> ({role})
    </div>
  );
}

function JoinRequest({
  request,
}: {
  request: JoinOrganizationRequest;
}) {
  const account = useCoState(Account, request._refs.account.id, {
    profile: {},
  });

  function handleAccept() {
    acceptJoinOrganizationRequest(request);
  }

  if (!account?.profile) return;

  return (
    <div className="px-4 py-4 sm:px-6 flex justify-between items-center">
      <strong className="font-medium">{account.profile.name}</strong>
      <button
        className="bg-blue-500 hover:bg-blue-700 text-white font-bold py-1 px-4 rounded"
        onClick={handleAccept}
      >
        Accept
      </button>
    </div>
  );
}
