import { co, z, setDefaultValidationMode } from "jazz-tools";
import { getRandomUsername } from "./util";

setDefaultValidationMode("strict");

export const Project = co
  .map({
    name: z.string(),
  })
  .withPermissions({
    onInlineCreate: "sameAsContainer",
  });

export const Organization = co
  .map({
    name: z.string(),
    projects: co.list(Project).withPermissions({
      onInlineCreate: "sameAsContainer",
    }),
  })
  .withPermissions({
    onInlineCreate: "newGroup",
  });
export type Organization = co.loaded<typeof Organization>;

export const DraftOrganization = co
  .map({
    name: z.optional(z.string()),
    projects: co.list(Project).withPermissions({
      onInlineCreate: "sameAsContainer",
    }),
  })
  .withPermissions({
    onInlineCreate: "newGroup",
  });
export type DraftOrganization = co.loaded<typeof DraftOrganization>;

export function validateDraftOrganization(org: DraftOrganization) {
  const errors: string[] = [];

  if (!org.name) {
    errors.push("Please enter a name.");
  }

  return {
    errors,
  };
}

export const JazzAccountRoot = co.map({
  organizations: co.list(Organization),
  draftOrganization: DraftOrganization,
});

export const JazzAccount = co
  .account({
    profile: co.profile().withPermissions({
      onCreate: (newGroup) => newGroup.makePublic(),
    }),
    root: JazzAccountRoot,
  })
  .withMigration(async (account) => {
    if (!account.$jazz.has("profile")) {
      account.$jazz.set("profile", {
        name: getRandomUsername(),
      });
    }

    if (!account.$jazz.has("root")) {
      const { profile } = await account.$jazz.ensureLoaded({
        resolve: {
          profile: true,
        },
      });

      account.$jazz.set("root", {
        draftOrganization: { projects: [] },
        organizations: [
          {
            name: profile.name ? `${profile.name}'s projects` : "Your projects",
            projects: [],
          },
        ],
      });
    }
  });

export const JazzAccountWithOrganizations = JazzAccount.resolved({
  root: { organizations: { $each: { $onError: "catch" } } },
});
