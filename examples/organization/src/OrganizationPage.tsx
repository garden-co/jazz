import { useAccount, useCoState } from "jazz-react";
import { ID } from "jazz-tools";
import { useParams } from "react-router";
import { Layout } from "./Layout.tsx";
import { CreateProject } from "./components/CreateProject.tsx";
import { Heading } from "./components/Heading.tsx";
import { InviteLink } from "./components/InviteLink.tsx";
import { OrganizationMembers } from "./components/OrganizationMembers.tsx";
import { RequestButton } from "./components/RequestInviteButton.tsx";
import { Requests } from "./components/Requests.tsx";
import { Organization } from "./schema.ts";

export function OrganizationPage() {
  const { me } = useAccount({ resolve: { root: { requests: true } } });
  console.log(me?.root.requests);
  const paramOrganizationId = useParams<{ organizationId: ID<Organization> }>()
    .organizationId;

  const organization = useCoState(Organization, paramOrganizationId, {
    resolve: { projects: true },
  });

  if (!paramOrganizationId) return <p>Invalid organization ID</p>;

  if (!organization && paramOrganizationId) {
    return (
      <Layout>
        <div className="grid gap-8">
          <Heading text="Request Access" />
          <RequestButton
            organization={{ id: paramOrganizationId } as Organization}
          />
        </div>
      </Layout>
    );
  } else if (organization) {
    const isAdmin = organization.mainGroup?.myRole() === "admin";

    return (
      <Layout>
        <div className="grid gap-8">
          <Heading text={`Welcome to ${organization.name} organization!`} />

          <div className="rounded-lg border shadow-sm bg-white dark:bg-stone-925">
            <div className="border-b px-4 py-5 sm:px-6">
              <div className="flex justify-between items-center">
                <h2>Members</h2>

                {isAdmin && <InviteLink organization={organization} />}
              </div>
            </div>
            <div className="divide-y">
              <OrganizationMembers organization={organization} />
            </div>
          </div>

          <div className="rounded-lg border shadow-sm bg-white dark:bg-stone-925">
            <div className="border-b px-4 py-5 sm:px-6">
              <h2>Projects</h2>
            </div>
            <div className="divide-y">
              {organization.projects.length > 0 ? (
                organization.projects.map((project) =>
                  project ? (
                    <strong
                      key={project.id}
                      className="px-4 py-5 sm:px-6 font-medium block"
                    >
                      {project.name}
                    </strong>
                  ) : null,
                )
              ) : (
                <p className="col-span-full text-center px-4 py-8 sm:px-6">
                  You have no projects yet.
                </p>
              )}
              <div className="p-4 sm:p-6">
                <CreateProject organization={organization} />
              </div>
            </div>
          </div>

          {isAdmin && (
            <div className="rounded-lg border shadow-sm bg-white dark:bg-stone-925">
              <div className="border-b px-4 py-5 sm:px-6">
                <h2>Invite Requests</h2>
                <Requests organization={organization} />
              </div>
            </div>
          )}
        </div>
      </Layout>
    );
  }
}
