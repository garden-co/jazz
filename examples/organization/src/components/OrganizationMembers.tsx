import { Group } from "jazz-tools";
import { Organization } from "../schema.ts";

export function OrganizationMembers({
  organization,
}: { organization: Organization }) {
  const group = organization._owner.castAs(Group);

  return (
    <>
      {group.members.map((member) => (
        <div key={member.id} className="px-4 py-5 sm:px-6">
          <strong className="font-medium">
            {member.account.profile?.name}
          </strong>{" "}
          ({member.role})
        </div>
      ))}
    </>
  );
}
