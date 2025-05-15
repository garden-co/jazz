import { Organization, Request, RequestsList, RequestsToJoin } from "@/schema";
import { useAccount, useCoState } from "jazz-react";
import { useCallback, useEffect, useState } from "react";

interface RequestButtonProps {
  organization: Organization;
}

export function RequestButton({ organization }: RequestButtonProps) {
  const { me } = useAccount({
    resolve: { root: { requests: true } },
  });
  console.log({ organization });
  const isAdmin = organization.mainGroup?.myRole() === "admin";

  const [error, setError] = useState<string | null>(null);
  const [isLoading, setIsLoading] = useState(false);
  const [hasRequested, setHasRequested] = useState(false);

  const globalRequests = useCoState(RequestsToJoin, me?.root.requests?.id, {
    resolve: { requests: true },
  });
  console.log(globalRequests);

  // Get the organization's requests using useCoState
  const orgRequests = useCoState(RequestsList, organization.requests?.id, {
    resolve: true,
  });

  // Check if user has already requested access
  useEffect(() => {
    if (me && orgRequests) {
      const userRequest = Object.values(orgRequests).find(
        (request) => request?.account?.id === me.id,
      );
      setHasRequested(!!userRequest);
    }
  }, [me, orgRequests]);

  const requestAccess = useCallback(() => {
    if (!me) {
      setError("You must be logged in to request access");
      return;
    }

    if (!me.root?.requests?.requests) {
      setError("Could not access requests container");
      return;
    }

    // if (!organization._owner) {
    //   setError("Organization has no owner");
    //   return;
    // }

    setIsLoading(true);
    try {
      console.log("Creating request...");
      // Create the request with the organization's owner as the owner
      console.log(organization.id);
      const request = Request.create(
        {
          account: me,
          organization: { _id: organization.id } as unknown as Organization,
          status: "pending",
          requestedAt: new Date(),
        },
        { owner: organization._owner },
      );

      console.log(request.toJSON());
      const globalRequests = me.root.requests.requests as RequestsList;

      // Add to organization's requests
      if (orgRequests) {
        orgRequests[request.id] = request;
      } else if (globalRequests) {
        globalRequests[request.id] = request;
        console.log(globalRequests.toJSON());
      } else {
        console.log("Organization has no requests container");
      }

      // // Add to global requests
      // globalRequests[request.id] = request;
      setHasRequested(true);
      setError(null);
    } catch (err) {
      console.error("Error creating request:", err);
      setError(
        `Failed to create request: ${err instanceof Error ? err.message : "Unknown error"}`,
      );
    } finally {
      setIsLoading(false);
    }
  }, [me, organization, orgRequests]);

  return (
    <div>
      {/* {isAdmin ? (
        <div className="px-3 py-1 mx-6"> 
          Organization Admin
          </div>
        ) : ( */}
      <button
        onClick={requestAccess}
        disabled={isLoading || hasRequested}
        className={`px-3 py-1 mx-6 ${
          isLoading || hasRequested
            ? "bg-gray-400 cursor-not-allowed"
            : "bg-blue-500 hover:bg-blue-600"
        } text-white rounded`}
      >
        {isLoading
          ? "Creating request..."
          : hasRequested
            ? "Invite requested"
            : "Request Invite to Organization"}
      </button>
      {/* )} */}
      {error && <p className="text-red-500 text-sm mt-1">{error}</p>}
    </div>
  );
}
