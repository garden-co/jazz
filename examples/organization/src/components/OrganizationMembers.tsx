import { Account, Group, Loaded } from "jazz-tools";
import { useAccount } from "jazz-tools/react";
import { Organization } from "../schema.ts";

export function OrganizationMembers({
  organization,
}: {
  organization: Loaded<typeof Organization>;
}) {
  const group = organization._owner.castAs(Group);

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

  const canRemoveMember = group.myRole() === "admin" && account.id !== me?.id;

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
