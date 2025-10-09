import { CoValueLoadingState } from "jazz-tools";
import { useAccount } from "jazz-tools/react";
import { Layout } from "./Layout.tsx";
import { CreateOrganization } from "./components/CreateOrganization.tsx";
import { Heading } from "./components/Heading.tsx";
import { JazzAccount } from "./schema";

export function HomePage() {
  const { me } = useAccount(JazzAccount, {
    resolve: {
      root: {
        organizations: {
          $each: { $onError: null },
        },
      },
    },
  });

  if (me.$jazzState !== CoValueLoadingState.LOADED) return;

  return (
    <Layout>
      <Heading text="Organizations example app" />

      <div className="rounded-lg border shadow-sm bg-white dark:bg-stone-925">
        <div className="border-b px-4 py-5 sm:px-6">
          <h2>Organizations</h2>
        </div>
        <div className="divide-y">
          {me.root.organizations.length > 0 ? (
            me.root.organizations.map((project) =>
              project.$jazzState === CoValueLoadingState.LOADED ? (
                <a
                  key={project.$jazz.id}
                  className="px-4 py-5 sm:px-6 font-medium block"
                  href={`/#/organizations/${project.$jazz.id}`}
                >
                  <strong>{project.name}</strong>
                </a>
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
