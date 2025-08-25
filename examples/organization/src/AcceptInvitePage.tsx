import { useAcceptInvite, useAccount } from "jazz-tools/react";
import { useNavigate } from "react-router";
import { JazzAccount, Organization } from "./schema.ts";

export function AcceptInvitePage() {
  const navigate = useNavigate();
  const { me } = useAccount(JazzAccount, {
    resolve: { root: { organizations: true } },
  });

  const onAccept = (organizationId: string) => {
    if (me?.root?.organizations) {
      Organization.load(organizationId).then((organization) => {
        if (organization) {
          // avoid duplicates
          const ids = me.root.organizations.map(
            (organization) => organization?.$jazz.id,
          );
          if (ids.includes(organizationId)) return;

          me.root.organizations.$jazz.push(organization);
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
