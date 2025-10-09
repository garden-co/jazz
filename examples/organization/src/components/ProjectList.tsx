import { CoValueLoadingState } from "jazz-tools";
import { useOrganizationSelector } from "./OrganizationProvider.ts";

export function ProjectList() {
  const projects = useOrganizationSelector({
    select: (organization) => {
      return organization.projects;
    },
  });

  if (projects.length === 0) {
    return (
      <p className="col-span-full text-center px-4 py-8 sm:px-6">
        You have no projects yet.
      </p>
    );
  }

  return (
    <>
      {projects.map((project) =>
        project.$jazzState === CoValueLoadingState.LOADED ? (
          <strong
            key={project.$jazz.id}
            className="px-4 py-5 sm:px-6 font-medium block"
          >
            {project.name}
          </strong>
        ) : null,
      )}
    </>
  );
}
