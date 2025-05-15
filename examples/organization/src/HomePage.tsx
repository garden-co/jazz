import { useAccount } from "jazz-react";
import { Layout } from "./Layout.tsx";
import { CreateOrganization } from "./components/CreateOrganization.tsx";
import { Heading } from "./components/Heading.tsx";
import { RequestButton } from "./components/RequestInviteButton.tsx";

export function HomePage() {
  const { me } = useAccount({
    resolve: { root: { organizations: true } },
  });

  if (!me?.root.organizations) return;

  return (
    <Layout>
      <Heading text="Organizations example app" />

      <div className="rounded-lg border shadow-sm bg-white dark:bg-stone-925">
        <div className="border-b px-4 py-5 sm:px-6">
          <h2>Organizations</h2>
        </div>
        <div className="divide-y">
          {me.root.organizations.length > 0 ? (
            me.root.organizations.map((project, index) =>
              project ? (
                <div
                  className="flex justify-between items-center"
                  key={`project-${index}-${project.name}`}
                >
                  <a
                    key={project.id}
                    className="px-4 py-5 sm:px-6 font-medium block"
                    href={`/#/organizations/${project.id}`}
                  >
                    <strong>{project.name}</strong>
                  </a>
                  {project.name !== "Your projects" ? (
                    <RequestButton organization={project} />
                  ) : null}
                </div>
              ) : null,
            )
          ) : (
            <p className="col-span-full text-center px-4 py-8 sm:px-6">
              You have no organizations yet.
            </p>
          )}
          <div className="p-4 sm:p-6">
            <CreateOrganization />
          </div>
        </div>
      </div>
    </Layout>
  );
}
