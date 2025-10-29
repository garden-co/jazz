import { co, z, Group } from "jazz-tools";

// #region Basic
export const Project = co.map({
  name: z.string(),
});

export const Organization = co.map({
  name: z.string(),

  // shared data between users of each organization
  projects: co.list(Project),
});

export const ListOfOrganizations = co.list(Organization);
// #endregion

// #region AddToRoot
export const JazzAccountRoot = co.map({
  organizations: co.list(Organization),
});

export const JazzAccount = co
  .account({
    root: JazzAccountRoot,
    profile: co.profile(),
  })
  .withMigration((account) => {
    if (!account.$jazz.has("root")) {
      // Using a Group as an owner allows you to give access to other users
      const organizationGroup = Group.create();

      const organizations = co.list(Organization).create([
        // Create the first Organization so users can start right away
        Organization.create(
          {
            name: "My organization",
            projects: co.list(Project).create([], organizationGroup),
          },
          organizationGroup,
        ),
      ]);
      account.$jazz.set("root", { organizations });
    }
  });
// #endregion
