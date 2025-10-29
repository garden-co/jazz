import { useAcceptInvite, useAccount } from "jazz-tools/react";
import { JazzAccount, Organization } from "./schema";

export function AcceptInvitePage() {
  const me = useAccount(JazzAccount, {
    resolve: { root: { organizations: { $each: { $onError: "catch" } } } },
  });

  const onAccept = (organizationId: string) => {
    if (me.$isLoaded) {
      Organization.load(organizationId).then((organization) => {
        if (organization) {
          // avoid duplicates
          const ids = me.root.organizations.map(
            (organization) => organization.$jazz.id,
          );
          if (ids.includes(organizationId)) return;

          me.root.organizations.$jazz.push(organization);
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
