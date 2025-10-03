import { Account, Group } from "jazz-tools";
import { useAccount } from "jazz-tools/react";
import { useOrganizationSelector } from "./OrganizationProvider.ts";

export function OrganizationMembers() {
  const group = useOrganizationSelector({
    select: (organization) => organization.$jazz.owner,
  });

  return (
    <>
      {group.members.map((member) => (
        <MemberItem
          key={member.id}
          account={member.account}
          role={member.role}
          group={group}
        />
      ))}
    </>
  );
}

function MemberItem({
  account,
  role,
  group,
}: {
  account: Account;
  role: string;
  group: Group;
}) {
  const { me } = useAccount();

  const canRemoveMember =
    group.myRole() === "admin" && account.$jazz.id !== me?.$jazz.id;

  function handleRemoveMember() {
    if (canRemoveMember && account) {
      group.removeMember(account);
    }
  }

  return (
    <div className="px-4 py-5 sm:px-6 flex justify-between items-center">
      <div>
        <strong className="font-medium">{account.profile?.name}</strong> ({role}
        )
      </div>
      {canRemoveMember && (
        <button
          className="bg-blue-500 hover:bg-blue-700 text-white font-bold py-2 px-4 rounded"
          onClick={handleRemoveMember}
        >
          Remove member
        </button>
      )}
    </div>
  );
}
