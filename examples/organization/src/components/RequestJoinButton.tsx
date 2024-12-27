import { ID } from "jazz-tools";
import { useAccount, useCoState } from "../main.tsx";
import { Organization, requestJoinOrganization } from "../schema.ts";

export function RequestJoinButton({
  organizationId,
}: { organizationId: ID<Organization> }) {
  const organization = useCoState(Organization, organizationId, {
    joinRequests: [{}],
  });

  const { me } = useAccount();

  if (!organization) return null;

  const alreadyRequested = organization.joinRequests.some(
    (request) => request._refs.account.id === me.id,
  );

  if (alreadyRequested) {
    return (
      <button
        type="button"
        className="inline-flex items-center gap-2 text-blue-500 dark:text-blue-400"
        disabled
      >
        Waiting for approval
      </button>
    );
  }

  return (
    <button
      type="button"
      className="inline-flex items-center gap-2 text-blue-500 dark:text-blue-400"
      onClick={() => {
        requestJoinOrganization(me, organization);
      }}
    >
      Request to join
    </button>
  );
}
