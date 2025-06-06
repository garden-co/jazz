import { Group, Loaded, co, z } from "jazz-tools";
import { getRandomUsername } from "./util";

export const Project = co.map({
  name: z.string(),
});

export const Organization = co.map({
  name: z.string(),
  projects: co.list(Project),
});

export const DraftOrganization = co
  .map({
    name: z.optional(z.string()),
    projects: co.list(Project),
  })
  .withHelpers((Self) => ({
    validate(org: Loaded<typeof Self>) {
      const errors: string[] = [];

      if (!org.name) {
        errors.push("Please enter a name.");
      }

      return {
        errors,
      };
    },
  }));

export const JazzAccountRoot = co.map({
  organizations: co.list(Organization),
  draftOrganization: DraftOrganization,
});

export const JazzAccount = co
  .account({
    profile: co.profile(),
    root: JazzAccountRoot,
  })
  .withMigration(async (account) => {
    if (account.profile === undefined) {
      const group = Group.create();
      account.profile = co.profile().create(
        {
          name: getRandomUsername(),
        },
        group,
      );
      group.addMember("everyone", "reader");
    }

    if (account.root === undefined) {
      const draftOrgGroup = Group.create();
      const draftOrganization = DraftOrganization.create(
        {
          projects: co.list(Project).create([], draftOrgGroup),
        },
        draftOrgGroup,
      );

      const defaultOrgGroup = Group.create();

      const { profile } = await account.ensureLoaded({
        resolve: {
          profile: true,
        },
      });

      const organizations = co.list(Organization).create([
        Organization.create(
          {
            name: profile.name ? `${profile.name}'s projects` : "Your projects",
            projects: co.list(Project).create([], defaultOrgGroup),
          },
          defaultOrgGroup,
        ),
      ]);

      account.root = JazzAccountRoot.create({
        draftOrganization,
        organizations,
      });
    }
  });
