import { useAcceptInvite, useAccount } from "jazz-react";
import { Account, ID } from "jazz-tools";
import { useEffect, useState } from "react";
import { useLocation, useNavigate } from "react-router";
import { Organization } from "./schema.ts";

export function AcceptInvitePage() {
  const navigate = useNavigate();
  const location = useLocation();
  const { me } = useAccount({ resolve: { root: { organizations: true } } });
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (me === null) {
      const currentPath = location.pathname + location.search + location.hash;
      navigate(`/?returnURL=${encodeURIComponent(currentPath)}`);
    }
  }, [me, navigate, location]);

  useEffect(() => {
    if (me && !me.root?.organizations) {
      setError("could not load your account. Please Try refreshing the page.");
    }
  }, [me]);

  const organizations = me?.root?.organizations;

  const onAccept = async (organizationId: ID<Organization>) => {
    if (!organizations) return;

    try {
      setError(null);
      const organization = await Organization.load(organizationId, {
        resolve: true,
      });

      if (!organization) {
        throw new Error("Failed to load organization");
      }

      if (!organization.mainGroup) {
        throw new Error("Organization has no main group");
      }
      // add invited user to organization
      organization.mainGroup.addMember(me as Account, "writer");

      const exists = organizations.some((org) => org?.id === organization.id);
      if (!exists) {
        organizations.push(organization);
        // pause to sync data, is there a better way to do this?
        await new Promise((resolve) => setTimeout(resolve, 1000));
      }

      navigate("/organizations/" + organizationId);
    } catch (error) {
      const errorMsg =
        error instanceof Error ? error.message : "an unknown error occured";
      setError(`Error accepting invite ${errorMsg}`);
    }

    if (organizations) {
      Organization.load(organizationId).then((organization) => {
        if (organization) {
          // avoid duplicates
          const ids = organizations.map((organization) => organization?.id);
          if (ids.includes(organizationId)) return;

          organizations.push(organization);
          navigate("/organizations/" + organizationId);
        }
      });
    }
  };

  useAcceptInvite({
    invitedObjectSchema: Organization,
    onAccept,
  });

  if (me === undefined) {
    return <LoadingView message="Loading..." />;
  }

  if (me === null) {
    return <LoadingView message="Redirecting to login..." />;
  }

  if (error) {
    return <LoadingView message={error} isError />;
  }

  return <LoadingView message="Accepting invite..." />;
}

// Reusable loading view component
function LoadingView({
  message,
  isError = false,
}: { message: string; isError?: boolean }) {
  return (
    <div className="flex items-center justify-center min-h-screen">
      <p className={`text-lg ${isError ? "text-red-600" : ""}`}>{message}</p>
    </div>
  );
}
