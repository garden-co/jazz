import { Account, CoList, CoMap, Group, co } from "jazz-tools";

export class Project extends CoMap {
  name = co.string;
}

export class ListOfProjects extends CoList.Of(co.ref(Project)) {}

// publicly visible org data
export class PublicOrganizationData extends CoMap {
  name = co.string;
  memberCount = co.number;
}

export class Request extends CoMap {
  account = co.ref(Account);
  organization = co.ref(Organization);
  status = co.literal("pending", "approved", "rejected");
  requestedAt = co.Date;
}

// this is all requests within one organization
export class RequestsList extends CoMap.Record(co.ref(Request)) {}

// The container of all the requests/approvals across all organizations
export class RequestsToJoin extends CoMap {
  writeOnlyInvite = co.string;
  requests = co.ref(RequestsList);
}

export class Organization extends CoMap {
  name = co.string;
  projects = co.ref(ListOfProjects);
  publicData = co.ref(PublicOrganizationData);
  requests = co.ref(RequestsList);
  mainGroup = co.ref(Group);

  static createNew(name: string, owner: Account): Organization {
    // the creater of the group is the admin
    const mainGroup = Group.create({ owner });
    mainGroup.addMember(owner, "admin");

    // the public group is available publicly because everyone is set as a reader
    const publicGroup = Group.create({ owner });
    publicGroup.addMember("everyone", "reader");
    publicGroup.extend(mainGroup);

    // Only admins can manage requests, but anyone can write them.
    const requestsGroup = Group.create({ owner });
    requestsGroup.addMember("everyone", "writer");
    requestsGroup.extend(mainGroup);

    // create the publicly available data for the org
    const publicData = PublicOrganizationData.create(
      {
        name,
        memberCount: 1,
      },
      { owner: publicGroup },
    );

    const requests = RequestsList.create({}, { owner: requestsGroup });

    const projects = ListOfProjects.create([], { owner: mainGroup });

    // Create the organization with the main group as owner
    return super.create(
      {
        name,
        projects,
        publicData,
        requests,
        mainGroup,
      },
      { owner: mainGroup },
    ) as Organization;
  }
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
  requests = co.ref(RequestsToJoin);
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
      const adminsGroup = Group.create({ owner: this });
      adminsGroup.addMember(this, "admin");

      const publicGroup = Group.create({ owner: this });
      publicGroup.addMember("everyone", "reader");
      publicGroup.extend(adminsGroup);

      const requestsGroup = Group.create({ owner: this });
      requestsGroup.addMember("everyone", "writer");
      requestsGroup.extend(adminsGroup);

      const organizations = ListOfOrganizations.create(
        [
          Organization.create(
            {
              name: this.profile?.name
                ? `${this.profile.name}'s projects`
                : "Your projects",
              projects: ListOfProjects.create([], initialOrganizationOwnership),
              requests: RequestsList.create({}, requestsGroup),
              publicData: PublicOrganizationData.create(
                {
                  name: this.profile?.name
                    ? `${this.profile.name}'s projects`
                    : "Your projects",
                  memberCount: 1,
                },
                { owner: publicGroup },
              ),
              mainGroup: adminsGroup,
            },
            initialOrganizationOwnership,
          ),
        ],
        { owner: this },
      );

      const requestsToJoin = RequestsToJoin.create(
        {
          writeOnlyInvite: "",
          requests: RequestsList.create({}, { owner: requestsGroup }),
        },
        { owner: requestsGroup },
      );

      this.root = JazzAccountRoot.create(
        {
          draftOrganization,
          organizations,
          requests: requestsToJoin,
        },
        { owner: this },
      );
    }
  }
}
