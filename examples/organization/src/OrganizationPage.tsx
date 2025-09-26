import { useParams } from "react-router";
import { Layout } from "./Layout.tsx";
import { CreateProject } from "./components/CreateProject.tsx";
import { Heading } from "./components/Heading.tsx";
import { InviteLink } from "./components/InviteLink.tsx";
import { OrganizationMembers } from "./components/OrganizationMembers.tsx";
import {
  OrganizationProvider,
  useOrganizationSelector,
} from "./components/OrganizationProvider.ts";
import { ProjectList } from "./components/ProjectList.tsx";

export function OrganizationPage() {
  const paramOrganizationId = useParams<{ organizationId: string }>()
    .organizationId;

  return (
    <OrganizationProvider
      id={paramOrganizationId}
      loadingFallback={<p>Loading organization...</p>}
      unavailableFallback={
        <div className="h-full flex items-center justify-center">
          <div className="text-center">
            <h1 className="text-2xl font-bold">
              You don't have access to this organization
            </h1>
            <a href="/#" className="text-blue-500">
              Go back to home
            </a>
          </div>
        </div>
      }
    >
      <OrganizationPageContent />
    </OrganizationProvider>
  );
}

function OrganizationPageContent() {
  const organizationName = useOrganizationSelector({
    select: (organization) => organization.name,
  });

  const isOrganizationAdmin = useOrganizationSelector({
    select: (organization) => organization.$jazz.owner?.myRole() === "admin",
  });

  return (
    <Layout>
      <div className="grid gap-8">
        <Heading text={`Welcome to ${organizationName} organization!`} />

        <div className="rounded-lg border shadow-sm bg-white dark:bg-stone-925">
          <div className="border-b px-4 py-5 sm:px-6">
            <div className="flex justify-between items-center">
              <h2>Members</h2>

              {isOrganizationAdmin && <InviteLink />}
            </div>
          </div>
          <div className="divide-y">
            <OrganizationMembers />
          </div>
        </div>

        <div className="rounded-lg border shadow-sm bg-white dark:bg-stone-925">
          <div className="border-b px-4 py-5 sm:px-6">
            <h2>Projects</h2>
          </div>
          <div className="divide-y">
            <ProjectList />
            <div className="p-4 sm:p-6">
              <CreateProject />
            </div>
          </div>
        </div>

        <div></div>
      </div>
    </Layout>
  );
}
