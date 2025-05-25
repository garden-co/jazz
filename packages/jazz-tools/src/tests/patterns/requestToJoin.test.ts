import { assert, describe, expect, test } from "vitest";
import { Account, CoList, CoMap, Group, ID, co, z } from "../../exports";
import { createJazzTestAccount, linkAccounts } from "../../testing";

const RequestToJoin = co.map({
  account: Account,
  status: z.literal(["pending", "approved", "rejected"]),
});

const RequestsMap = co.record(z.string(), RequestToJoin);

const RequestsStatus = co.record(
  z.string(),
  z.literal(["pending", "approved", "rejected"]),
);

const Organization = co.map({
  name: z.string(),
  requests: RequestsMap,
  statuses: RequestsStatus,
  projects: co.list(z.string()),
  mainGroup: Group,
  adminsGroup: Group,
});

async function setup() {
  const admin1 = await createJazzTestAccount();
  const admin2 = await createJazzTestAccount();
  const user1 = await createJazzTestAccount();
  const user2 = await createJazzTestAccount();

  await linkAccounts(admin1, admin2);
  await linkAccounts(admin1, user1);
  await linkAccounts(admin1, user2);
  await linkAccounts(admin2, user1);
  await linkAccounts(admin2, user2);

  // The organization info are public
  const adminsGroup = Group.create(admin1);
  adminsGroup.addMember(admin2, "admin");

  const publicGroup = Group.create(admin1);
  publicGroup.addMember("everyone", "reader");
  publicGroup.addMember(adminsGroup);

  // Everyone can write to requests, but only admins can read
  const requestsGroup = Group.create(admin1);
  requestsGroup.addMember("everyone", "writeOnly");
  requestsGroup.addMember(adminsGroup);

  const organizationGroup = Group.create(admin1);
  organizationGroup.addMember(adminsGroup);

  const organization = Organization.create(
    {
      name: "My Organization",
      requests: RequestsMap.create({}, requestsGroup),

      // To simulate the resource to share
      projects: co.list(z.string()).create([], organizationGroup),

      // Statuses are private to admins
      // Used to make the requests statues readable only to them
      // and not editable by anyone else
      // The status info inside RequestToJoin is only used to notify the user of the status change
      // but this is the source of truth for admins
      statuses: RequestsStatus.create({}, adminsGroup),
      mainGroup: organizationGroup,
      adminsGroup,
    },
    publicGroup,
  );

  const organizationId = organization.id;

  await admin1.waitForAllCoValuesSync();

  return {
    admin1,
    admin2,
    user1,
    user2,
    organizationId,
  };
}

async function sendRequestToJoin(organizationId: string, account: Account) {
  const organization = await Organization.load(organizationId, {
    resolve: { requests: true, adminsGroup: true },
    loadAs: account,
  });

  if (!organization) {
    throw new Error("RequestsMap not found");
  }

  const group = Group.create(account);
  group.addMember(organization.adminsGroup);

  const request = RequestToJoin.create(
    {
      account,
      status: "pending",
    },
    group,
  );

  organization.requests[account.id] = request;

  await account.waitForAllCoValuesSync();

  return request;
}

async function approveRequest(
  organizationId: string,
  admin: Account,
  user: Account,
) {
  const organization = await Organization.load(organizationId, {
    loadAs: admin,
    resolve: { statuses: true, requests: { $each: true }, mainGroup: true },
  });

  if (!organization) {
    throw new Error("Organization not found");
  }

  const request = organization.requests[user.id];

  if (
    organization.statuses[user.id] === "approved" ||
    organization.statuses[user.id] === "rejected"
  ) {
    throw new Error("Request already processed");
  }

  if (!request) {
    throw new Error("Request not found");
  }

  request.status = "approved";
  organization.statuses[user.id] = "approved";

  organization.mainGroup.addMember(user, "writer");

  await admin.waitForAllCoValuesSync();
}

async function rejectRequest(
  organizationId: string,
  admin: Account,
  user: Account,
) {
  const organization = await Organization.load(organizationId, {
    loadAs: admin,
    resolve: { statuses: true, requests: { $each: true } },
  });

  if (!organization) {
    throw new Error("Organization not found");
  }

  const request = organization.requests[user.id];

  if (
    organization.statuses[user.id] === "approved" ||
    organization.statuses[user.id] === "rejected"
  ) {
    throw new Error("Request already processed");
  }

  if (!request) {
    throw new Error("Request not found");
  }

  request.status = "rejected";
  organization.statuses[user.id] = "rejected";

  await admin.waitForAllCoValuesSync();
}

describe("Request to join", () => {
  test("accepting a request gives access to the projects", async () => {
    const { admin1, user1, organizationId } = await setup();

    const organization = await Organization.load(organizationId, {
      loadAs: admin1,
      resolve: {
        requests: { $each: true },
        statuses: { $each: true },
        projects: true,
      },
    });

    assert(organization);

    await sendRequestToJoin(organizationId, user1);

    await approveRequest(organizationId, admin1, user1);

    const projectsOnUser = await co
      .list(z.string())
      .load(organization.projects.id, {
        loadAs: user1,
      });

    assert(projectsOnUser);

    projectsOnUser.push("project1");

    expect(projectsOnUser[0]).toBe("project1");
  });

  test("rejecting a request does not give access to the projects", async () => {
    const { admin1, user1, organizationId } = await setup();

    const organization = await Organization.load(organizationId, {
      loadAs: admin1,
      resolve: {
        requests: { $each: true },
        statuses: { $each: true },
        projects: true,
      },
    });

    assert(organization);

    await sendRequestToJoin(organizationId, user1);
    await rejectRequest(organizationId, admin1, user1);

    const projectsOnUser = await co
      .list(z.string())
      .load(organization.projects.id, {
        loadAs: user1,
      });

    expect(projectsOnUser).toBeNull();
  });

  test("admin2 can see the status of the requests", async () => {
    const { admin1, admin2, user1, organizationId } = await setup();

    const request = await sendRequestToJoin(organizationId, user1);
    await approveRequest(organizationId, admin1, user1);

    const organization = await Organization.load(organizationId, {
      loadAs: admin2,
      resolve: { requests: { $each: true }, statuses: { $each: true } },
    });

    assert(organization);
    expect(organization.statuses[user1.id]).toBe("approved");
    const requestOnAdmin2 = await RequestToJoin.load(request.id, {
      loadAs: admin2,
    });
    assert(requestOnAdmin2);
    expect(requestOnAdmin2.status).toBe("approved");
  });

  test("user2 can't see the requests of user1", async () => {
    const { user1, user2, organizationId } = await setup();

    const request = await sendRequestToJoin(organizationId, user1);

    const requestOnUser2 = await RequestToJoin.load(request.id, {
      loadAs: user2,
    });

    // With the writeOnly permission, the user can download the request
    // but not it's content
    expect(requestOnUser2).toBeNull();
  });
});
