import { createInviteLink, parseInviteLink } from "jazz-react";
import { Account, CoList, CoMap, Group, co } from "jazz-tools";

export class Project extends CoMap {
  name = co.string;
}

export class ListOfProjects extends CoList.Of(co.ref(Project)) {}

export class Organization extends CoMap {
  // everyone is a reader
  name = co.string;
  content = co.ref(OrganizationContent); // limited access
  joinRequests = co.ref(ListOfJoinOrganizationRequests); // writeOnly access
  joinRequestsInviteLink = co.string;
}

export class OrganizationContent extends CoMap {
  projects = co.ref(ListOfProjects);
}

export class DraftOrganization extends CoMap {
  name = co.optional.string;

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

export class JoinOrganizationRequest extends CoMap {
  organization = co.ref(Organization);
  account = co.ref(Account);
}

export class ListOfOrganizations extends CoList.Of(co.ref(Organization)) {}

export class ListOfJoinOrganizationRequests extends CoList.Of(
  co.ref(JoinOrganizationRequest),
) {
  removeRequest(request: JoinOrganizationRequest) {
    const index = this.findIndex((r) => r?.id === request.id);
    if (index !== -1) {
      this.splice(index, 1);
    }
  }
}

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
          name: "",
        },
        draftOrganizationOwnership,
      );

      const organizations = ListOfOrganizations.create(
        [
          createOrganization(
            this,
            this.profile?.name
              ? `${this.profile.name}'s projects`
              : "Your projects",
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

export function createOrganization(account: Account, name: string) {
  const organizationOwnership = {
    owner: Group.create({ owner: account }),
  };

  // We give read only access to everyone so that guests can see the organization name
  // and request to join
  organizationOwnership.owner.addMember("everyone", "reader");

  const joinRequestsGroup = Group.create({ owner: account });

  const joinRequests = ListOfJoinOrganizationRequests.create([], {
    owner: joinRequestsGroup,
  });

  // We give write only access to the join requests so that only the organization admins
  // can see and manage join requests
  const joinRequestsInviteLink = createInviteLink(joinRequests, "writeOnly");

  const contentOwnership = {
    owner: Group.create({ owner: account }),
  };

  const projects = ListOfProjects.create([], contentOwnership);
  const content = OrganizationContent.create({ projects }, contentOwnership);

  const organization = Organization.create(
    { name, joinRequests, joinRequestsInviteLink, content },
    organizationOwnership,
  );

  return organization;
}

export async function acceptJoinOrganizationRequest(
  joinRequest: JoinOrganizationRequest,
) {
  const result = await joinRequest.ensureLoaded({
    organization: {
      joinRequests: [],
      content: {},
    },
    account: {},
  });

  if (!result) return;

  const { organization, account } = result;

  organization.joinRequests.removeRequest(joinRequest);

  const organizationContentGroup = organization.content._owner.castAs(Group);
  organizationContentGroup.addMember(account, "writer");
}

export async function requestJoinOrganization(
  account: JazzAccount,
  organization: Organization,
) {
  const parsedLink = parseInviteLink<ListOfJoinOrganizationRequests>(
    organization.joinRequestsInviteLink,
  );

  if (!parsedLink) return;

  const joinRequests = await account.acceptInvite(
    parsedLink.valueID,
    parsedLink.inviteSecret,
    ListOfJoinOrganizationRequests,
  );

  if (!joinRequests) return;

  const joinRequestsGroup = joinRequests._owner.castAs(Group);
  const joinRequest = JoinOrganizationRequest.create(
    { organization, account },
    { owner: joinRequestsGroup },
  );

  joinRequests.push(joinRequest);

  const result = await account.ensureLoaded({ root: { organizations: [] } });

  if (!result) return;

  const { root } = result;

  root.organizations.push(organization);
}
