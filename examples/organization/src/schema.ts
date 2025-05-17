import { Account, CoList, CoMap, Group, Profile, coField } from "jazz-tools";
import { getRandomUsername } from "./util";

export class Project extends CoMap {
  name = coField.string;
}

export class ListOfProjects extends CoList.Of(coField.ref(Project)) {}

export class Organization extends CoMap {
  name = coField.string;
  projects = coField.ref(ListOfProjects);
}

export class DraftOrganization extends CoMap {
  name = coField.optional.string;
  projects = coField.ref(ListOfProjects);

  validate() {
    const errors: string[] = [];

    if (!this.name) {
      errors.push("Please enter a name.");
    }

    return {
      errors,
    };
  }
}

export class ListOfOrganizations extends CoList.Of(coField.ref(Organization)) {}

export class JazzAccountRoot extends CoMap {
  organizations = coField.ref(ListOfOrganizations);
  draftOrganization = coField.ref(DraftOrganization);
}

export class JazzAccount extends Account {
  root = coField.ref(JazzAccountRoot);

  async migrate(this: JazzAccount) {
    if (this.profile === undefined) {
      const group = Group.create();
      this.profile = Profile.create(
        {
          name: getRandomUsername(),
        },
        group,
      );
      group.addMember("everyone", "reader");
    }

    if (this.root === undefined) {
      const draftOrgGroup = Group.create();
      const draftOrganization = DraftOrganization.create(
        {
          projects: ListOfProjects.create([], draftOrgGroup),
        },
        draftOrgGroup,
      );

      const defaultOrgGroup = Group.create();

      const { profile } = await this.ensureLoaded({
        resolve: {
          profile: true,
        },
      });

      const organizations = ListOfOrganizations.create([
        Organization.create(
          {
            name: profile.name ? `${profile.name}'s projects` : "Your projects",
            projects: ListOfProjects.create([], defaultOrgGroup),
          },
          defaultOrgGroup,
        ),
      ]);

      this.root = JazzAccountRoot.create({
        draftOrganization,
        organizations,
      });
    }
  }
}
