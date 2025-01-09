import { Account, CoList, CoMap, Group, co } from "jazz-tools";

export class Project extends CoMap {
  name = co.string;
}

export class ListOfProjects extends CoList.Of(co.ref(Project)) {}

export class Organization extends CoMap {
  name = co.string;
  projects = co.ref(ListOfProjects);
}

export class DraftOrganization extends CoMap {
  name = co.optional.string;
  projects = co.ref(ListOfProjects);

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

export class ListOfOrganizations extends CoList.Of(co.ref(Organization)) {}

export class JazzAccountRoot extends CoMap {
  organizations = co.ref(ListOfOrganizations);
  draftOrganization = co.ref(DraftOrganization);
}

export class JazzAccount extends Account {
  root = co.ref(JazzAccountRoot);

  async migrate() {
    if (!this._refs.root) {
      const draftOrganizationOwnership = {
        owner: Group.create({ owner: this }),
      };
      const draftOrganization = DraftOrganization.create(
        {
          projects: ListOfProjects.create([], draftOrganizationOwnership),
        },
        draftOrganizationOwnership,
      );

      const initialOrganizationOwnership = {
        owner: Group.create({ owner: this }),
      };
      const organizations = ListOfOrganizations.create(
        [
          Organization.create(
            {
              name: this.profile?.name
                ? `${this.profile.name}'s projects`
                : "Your projects",
              projects: ListOfProjects.create([], initialOrganizationOwnership),
            },
            initialOrganizationOwnership,
          ),
        ],
        { owner: this },
      );

      this.root = JazzAccountRoot.create(
        {
          draftOrganization,
          organizations,
        },
        { owner: this },
      );
    }
  }
}
