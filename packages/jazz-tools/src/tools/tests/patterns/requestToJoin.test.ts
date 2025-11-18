import { assert, describe, expect, test } from "vitest";
import { Account, Group, co, z } from "../../exports";
import { CoValueLoadingState } from "../../internal.js";
import {
  createJazzTestAccount,
  linkAccounts,
  setupJazzTestSync,
} from "../../testing";
import { assertLoaded } from "../utils.js";

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
  await setupJazzTestSync();

  const admin1 = await createJazzTestAccount();
  const admin2 = await createJazzTestAccount();
  const user1 = await createJazzTestAccount();
  const user2 = await createJazzTestAccount();

  // TODO: with this setting the waitForAllCoValuesSync gets stuck
  // https://github.com/garden-co/jazz/issues/2874
  // await linkAccounts(admin1, admin2);
  // await linkAccounts(admin1, user1);
  // await linkAccounts(admin1, user2);
  // await linkAccounts(admin2, user1);
  // await linkAccounts(admin2, user2);

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

  const organizationId = organization.$jazz.id;

  await admin1.$jazz.waitForAllCoValuesSync();

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

  assertLoaded(organization);

  const group = Group.create(account);
  group.addMember(organization.adminsGroup);

  const request = RequestToJoin.create(
    {
      account,
      status: "pending",
    },
    group,
  );

  organization.requests.$jazz.set(account.$jazz.id, request);

  await account.$jazz.waitForAllCoValuesSync();

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

  assertLoaded(organization);

  const request = organization.requests[user.$jazz.id];

  if (
    organization.statuses[user.$jazz.id] === "approved" ||
    organization.statuses[user.$jazz.id] === "rejected"
  ) {
    throw new Error("Request already processed");
  }

  if (!request) {
    throw new Error("Request not found");
  }

  request.$jazz.set("status", "approved");
  organization.statuses.$jazz.set(user.$jazz.id, "approved");

  organization.mainGroup.addMember(user, "writer");

  await admin.$jazz.waitForAllCoValuesSync();
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

  assertLoaded(organization);

  const request = organization.requests[user.$jazz.id];

  if (
    organization.statuses[user.$jazz.id] === "approved" ||
    organization.statuses[user.$jazz.id] === "rejected"
  ) {
    throw new Error("Request already processed");
  }

  if (!request) {
    throw new Error("Request not found");
  }

  request.$jazz.set("status", "rejected");
  organization.statuses.$jazz.set(user.$jazz.id, "rejected");

  await admin.$jazz.waitForAllCoValuesSync();
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

    assertLoaded(organization);

    await sendRequestToJoin(organizationId, user1);

    await approveRequest(organizationId, admin1, user1);

    const projectsOnUser = await co
      .list(z.string())
      .load(organization.projects.$jazz.id, {
        loadAs: user1,
      });

    assertLoaded(projectsOnUser);

    projectsOnUser.$jazz.push("project1");

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

    assertLoaded(organization);

    await sendRequestToJoin(organizationId, user1);
    await rejectRequest(organizationId, admin1, user1);

    const projectsOnUser = await co
      .list(z.string())
      .load(organization.projects.$jazz.id, {
        loadAs: user1,
      });

    expect(projectsOnUser.$jazz.loadingState).toBe(
      CoValueLoadingState.UNAUTHORIZED,
    );
  });

  test("admin2 can see the status of the requests", async () => {
    const { admin1, admin2, user1, organizationId } = await setup();

    const request = await sendRequestToJoin(organizationId, user1);
    await approveRequest(organizationId, admin1, user1);

    const organization = await Organization.load(organizationId, {
      loadAs: admin2,
      resolve: { requests: { $each: true }, statuses: { $each: true } },
    });

    assertLoaded(organization);
    expect(organization.statuses[user1.$jazz.id]).toBe("approved");
    const requestOnAdmin2 = await RequestToJoin.load(request.$jazz.id, {
      loadAs: admin2,
    });
    assertLoaded(requestOnAdmin2);
    expect(requestOnAdmin2.status).toBe("approved");
  });

  test("user2 can't see the requests of user1", async () => {
    const { user1, user2, organizationId } = await setup();

    const request = await sendRequestToJoin(organizationId, user1);

    const requestOnUser2 = await RequestToJoin.load(request.$jazz.id, {
      loadAs: user2,
    });

    // With the writeOnly permission, the user can download the request
    // but not its content
    expect(requestOnUser2.$jazz.loadingState).toBe(
      CoValueLoadingState.UNAUTHORIZED,
    );
  });
});
