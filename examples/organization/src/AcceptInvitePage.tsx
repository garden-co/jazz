import { useAcceptInvite, useAccount } from "jazz-react";
import { ID } from "jazz-tools";
import { useNavigate } from "react-router";
import { Organization } from "./schema.ts";

export function AcceptInvitePage() {
  const navigate = useNavigate();
  const { me } = useAccount({ resolve: { root: { organizations: true } } });

  const onAccept = (organizationId: ID<Organization>) => {
    if (me?.root?.organizations) {
      Organization.load(organizationId).then((organization) => {
        if (organization) {
          // avoid duplicates
          const ids = me.root.organizations.map(
            (organization) => organization?.id,
          );
          if (ids.includes(organizationId)) return;

          me.root.organizations.push(organization);
          navigate("/organizations/" + organizationId);
        }
      });
    }
  };

  useAcceptInvite({
    invitedObjectSchema: Organization,
    onAccept,
  });

  return <p>Accepting invite...</p>;
}
