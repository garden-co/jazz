import { Organization, Request, RequestsList } from "@/schema";
import { useAccount, useCoState } from "jazz-react";
import { Group } from "jazz-tools";
import { useCallback } from "react";

interface RequestsProps {
  organization: Organization;
}

export function Requests({ organization }: RequestsProps) {
  const { me } = useAccount({
    resolve: {
      root: {
        requests: {
          requests: {
            $each: true,
          },
        },
      },
    },
  });

  // Get the organization's requests using useCoState
  const orgRequests = useCoState(RequestsList, organization.requests?.id, {
    resolve: true,
  });

  const handleRequest = useCallback(
    (request: Request, approved: boolean) => {
      if (
        !organization._owner?.castAs(Group) ||
        !request.account ||
        !orgRequests
      )
        return;

      try {
        // Update the request's status directly
        request.status = approved ? "approved" : "rejected";

        // Ensure the request stays in the container
        orgRequests[request.id] = request;

        if (approved) {
          // Add the requester as a writer to the organization's group
          const organizationGroup = organization._owner.castAs(Group);
          const requester = request.account;

          // Remove any existing role and add as writer
          organizationGroup.removeMember(requester);
          organizationGroup.addMember(requester, "writer");

          if (organization.publicData) {
            organization.publicData.memberCount += 1;
          }
        }
      } catch (error) {
        console.error("Error handling request:", error);
      }
    },
    [organization, orgRequests],
  );

  // Get all requests for this organization
  const organizationRequests = Object.values(orgRequests || {}) as Request[];

  return (
    <div className="divide-y">
      {organizationRequests.length === 0 ? (
        <p className="p-4 text-gray-500">No requests</p>
      ) : (
        organizationRequests.map((request) => {
          if (!request) return null;
          const account = request.account;
          return (
            <div key={request.id} className="flex items-center gap-4 p-4">
              <div className="flex-1">
                <p className="font-medium">
                  {account?.profile?.name || "Anonymous"}
                </p>
                <p className="text-sm text-gray-500">
                  Requested at: {request.requestedAt.toLocaleString()}
                </p>
                <p className="text-sm text-gray-500">
                  Status: {request.status}
                </p>
              </div>
              {request.status === "pending" && (
                <div className="flex gap-2">
                  <button
                    onClick={() => handleRequest(request, true)}
                    className="px-3 py-1 bg-green-500 text-white rounded hover:bg-green-600"
                  >
                    Approve
                  </button>
                  <button
                    onClick={() => handleRequest(request, false)}
                    className="px-3 py-1 bg-red-500 text-white rounded hover:bg-red-600"
                  >
                    Reject
                  </button>
                </div>
              )}
            </div>
          );
        })
      )}
    </div>
  );
}
