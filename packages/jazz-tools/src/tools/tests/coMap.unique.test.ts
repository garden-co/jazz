import { cojsonInternals } from "cojson";
import { assert, beforeEach, describe, expect, test } from "vitest";
import {
  assertLoaded,
  setupJazzTestSync,
  createJazzTestAccount,
  runWithoutActiveAccount,
} from "../testing";
import {
  CoValueLoadingState,
  Group,
  co,
  activeAccountContext,
  unstable_loadUnique,
} from "../internal";
import { z } from "../exports";
import { expectValidationError } from "./utils";

beforeEach(async () => {
  cojsonInternals.CO_VALUE_LOADING_CONFIG.RETRY_DELAY = 1000;

  await setupJazzTestSync();

  await createJazzTestAccount({
    isCurrentActiveAccount: true,
    creationProps: { name: "Hermes Puggington" },
  });
});

describe("Creating and finding unique CoMaps", async () => {
  test("Creating and finding unique CoMaps", async () => {
    const group = Group.create();

    const Person = co.map({
      name: z.string(),
      _height: z.number(),
      birthday: z.date(),
      color: z.string(),
    });

    const alice = Person.create(
      {
        name: "Alice",
        _height: 100,
        birthday: new Date("1990-01-01"),
        color: "red",
      },
      { owner: group, unique: { name: "Alice" } },
    );

    const foundAlice = await Person.loadUnique(
      { name: "Alice" },
      group.$jazz.id,
    );
    expect(foundAlice).toEqual(alice);
  });

  test("should work with unstable_loadUnique", async () => {
    const group = Group.create();

    const Person = co.map({
      name: z.string(),
      _height: z.number(),
      birthday: z.date(),
      color: z.string(),
    });

    const alice = Person.create(
      {
        name: "Alice",
        _height: 100,
        birthday: new Date("1990-01-01"),
        color: "red",
      },
      { owner: group, unique: { name: "Alice" } },
    );

    const foundAlice = await unstable_loadUnique(Person, {
      unique: { name: "Alice" },
      owner: group,
    });
    expect(foundAlice).toEqual(alice);
  });

  test("should upsert with unstable_loadUnique", async () => {
    const group = Group.create();

    const Person = co.map({
      name: z.string(),
      _height: z.number(),
      birthday: z.date(),
      color: z.string(),
    });

    const alice = await unstable_loadUnique(Person, {
      unique: { name: "Alice" },
      onCreateWhenMissing: () => {
        Person.create(
          {
            name: "Alice",
            _height: 100,
            birthday: new Date("1990-01-01"),
            color: "red",
          },
          { owner: group, unique: { name: "Alice" } },
        );
      },
      owner: group,
    });
    assertLoaded(alice);
    expect(alice.name).toEqual("Alice");
  });

  test("manual upserting pattern", async () => {
    // Schema
    const Event = co.map({
      title: z.string(),
      identifier: z.string(),
      external_id: z.string(),
    });

    // Data
    const sourceData = {
      title: "Test Event Title",
      identifier: "test-event-identifier",
      _id: "test-event-external-id",
    };
    const workspace = Group.create();

    // Pattern
    let activeEvent = await Event.loadUnique(
      { identifier: sourceData.identifier },
      workspace.$jazz.id,
    );
    if (!activeEvent.$isLoaded) {
      activeEvent = Event.create(
        {
          title: sourceData.title,
          identifier: sourceData.identifier,
          external_id: sourceData._id,
        },
        workspace,
      );
    } else {
      activeEvent.$jazz.applyDiff({
        title: sourceData.title,
        identifier: sourceData.identifier,
        external_id: sourceData._id,
      });
    }
    expect(activeEvent).toEqual({
      title: sourceData.title,
      identifier: sourceData.identifier,
      external_id: sourceData._id,
    });
  });

  test("upserting a non-existent value", async () => {
    // Schema
    const Event = co.map({
      title: z.string(),
      identifier: z.string(),
      external_id: z.string(),
    });

    // Data
    const sourceData = {
      title: "Test Event Title",
      identifier: "test-event-identifier",
      _id: "test-event-external-id",
    };
    const workspace = Group.create();

    // Upserting
    const activeEvent = await Event.upsertUnique({
      value: {
        title: sourceData.title,
        identifier: sourceData.identifier,
        external_id: sourceData._id,
      },
      unique: sourceData.identifier,
      owner: workspace,
    });
    expect(activeEvent).toEqual({
      title: sourceData.title,
      identifier: sourceData.identifier,
      external_id: sourceData._id,
    });
  });

  test("upserting should validate input against schema on creation", async () => {
    const group = Group.create();
    const Event = co.map({
      title: z.string(),
    });

    await expectValidationError(async () => {
      await Event.upsertUnique({
        value: {
          // @ts-expect-error - number is not a string
          title: 123,
        },
        unique: "test-event-identifier",
        owner: group,
      });
    });
  });

  test("upserting should validate input against schema on update", async () => {
    const group = Group.create();
    const Event = co.map({
      title: z.string(),
    });

    await Event.upsertUnique({
      value: {
        title: "123",
      },
      unique: "test-event-identifier",
      owner: group,
    });

    await expectValidationError(async () => {
      await Event.upsertUnique({
        value: {
          // @ts-expect-error - number is not a string
          title: 456,
        },
        unique: "test-event-identifier",
        owner: group,
      });
    });
  });

  test("upserting a existent value without enough permissions should not throw", async () => {
    const Event = co.map({
      title: z.string(),
      identifier: z.string(),
      external_id: z.string(),
    });

    const sourceData = {
      title: "Test Event Title",
      identifier: "test-event-identifier",
      _id: "test-event-external-id",
    };
    const workspace = Group.create();

    const initialEvent = await Event.upsertUnique({
      value: {
        title: sourceData.title,
        identifier: sourceData.identifier,
        external_id: sourceData._id,
      },
      unique: sourceData.identifier,
      owner: workspace,
    });

    const alice = await createJazzTestAccount({
      isCurrentActiveAccount: true,
    });

    const workspaceOnAlice = await Group.load(workspace.$jazz.id, {
      loadAs: alice,
    });
    assertLoaded(workspaceOnAlice);

    const eventOnAlice = await Event.upsertUnique({
      value: {
        title: sourceData.title,
        identifier: sourceData.identifier,
        external_id: sourceData._id,
      },
      unique: sourceData.identifier,
      owner: workspaceOnAlice,
    });
    expect(eventOnAlice.$isLoaded).toBe(false);
    expect(eventOnAlice.$jazz.loadingState).toBe(
      CoValueLoadingState.UNAUTHORIZED,
    );
  });

  test("upserting without an active account", async () => {
    const account = activeAccountContext.get();

    // Schema
    const Event = co.map({
      title: z.string(),
      identifier: z.string(),
      external_id: z.string(),
    });

    // Data
    const sourceData = {
      title: "Test Event Title",
      identifier: "test-event-identifier",
      _id: "test-event-external-id",
    };

    const activeEvent = await runWithoutActiveAccount(() => {
      return Event.upsertUnique({
        value: {
          title: sourceData.title,
          identifier: sourceData.identifier,
          external_id: sourceData._id,
        },
        unique: sourceData.identifier,
        owner: account,
      });
    });

    expect(activeEvent).toEqual({
      title: sourceData.title,
      identifier: sourceData.identifier,
      external_id: sourceData._id,
    });

    assertLoaded(activeEvent);

    expect(activeEvent.$jazz.owner).toEqual(account);
  });

  test("upserting an existing value", async () => {
    // Schema
    const Event = co.map({
      title: z.string(),
      identifier: z.string(),
      external_id: z.string(),
    });

    // Data
    const oldSourceData = {
      title: "Old Event Title",
      identifier: "test-event-identifier",
      _id: "test-event-external-id",
    };
    const newSourceData = {
      title: "New Event Title",
      identifier: "test-event-identifier",
      _id: "test-event-external-id",
    };
    expect(oldSourceData.identifier).toEqual(newSourceData.identifier);
    const workspace = Group.create();
    const oldActiveEvent = Event.create(
      {
        title: oldSourceData.title,
        identifier: oldSourceData.identifier,
        external_id: oldSourceData._id,
      },
      workspace,
    );

    // Upserting
    const activeEvent = await Event.upsertUnique({
      value: {
        title: newSourceData.title,
        identifier: newSourceData.identifier,
        external_id: newSourceData._id,
      },
      unique: newSourceData.identifier,
      owner: workspace,
    });
    expect(activeEvent).toEqual({
      title: newSourceData.title,
      identifier: newSourceData.identifier,
      external_id: newSourceData._id,
    });
    expect(activeEvent).not.toEqual(oldActiveEvent);
  });

  test("upserting a non-existent value with resolve", async () => {
    const Project = co.map({
      name: z.string(),
    });
    const Organisation = co.map({
      name: z.string(),
      projects: co.list(Project),
    });
    const workspace = Group.create();

    const myOrg = await Organisation.upsertUnique({
      value: {
        name: "My organisation",
        projects: co.list(Project).create(
          [
            Project.create(
              {
                name: "My project",
              },
              workspace,
            ),
          ],
          workspace,
        ),
      },
      unique: { name: "My organisation" },
      owner: workspace,
      resolve: {
        projects: {
          $each: true,
        },
      },
    });
    assertLoaded(myOrg);
    expect(myOrg.name).toEqual("My organisation");
    expect(myOrg.projects.length).toBe(1);
    expect(myOrg.projects[0]).toMatchObject({
      name: "My project",
    });
  });

  test("upserting an existing value with resolve", async () => {
    const Project = co.map({
      name: z.string(),
    });
    const Organisation = co.map({
      name: z.string(),
      projects: co.list(Project),
    });
    const workspace = Group.create();
    const initialProject = await Project.upsertUnique({
      value: {
        name: "My project",
      },
      unique: { unique: "First project" },
      owner: workspace,
    });
    assertLoaded(initialProject);
    expect(initialProject.name).toEqual("My project");

    const myOrg = await Organisation.upsertUnique({
      value: {
        name: "My organisation",
        projects: co.list(Project).create([initialProject], workspace),
      },
      unique: { name: "My organisation" },
      owner: workspace,
      resolve: {
        projects: {
          $each: true,
        },
      },
    });
    assertLoaded(myOrg);
    expect(myOrg.name).toEqual("My organisation");
    expect(myOrg.projects.length).toBe(1);
    expect(myOrg.projects.at(0)?.name).toEqual("My project");

    const updatedProject = await Project.upsertUnique({
      value: {
        name: "My updated project",
      },
      unique: { unique: "First project" },
      owner: workspace,
    });

    assertLoaded(updatedProject);
    expect(updatedProject).toEqual(initialProject);
    expect(updatedProject.name).toEqual("My updated project");
    expect(myOrg.projects.length).toBe(1);
    expect(myOrg.projects.at(0)?.name).toEqual("My updated project");
  });

  test("upserting a partially loaded value on an new value with resolve", async () => {
    const Project = co.map({
      name: z.string(),
    });
    const Organisation = co.map({
      name: z.string(),
      projects: co.list(Project),
    });
    const publicAccess = Group.create();
    publicAccess.addMember("everyone", "writer");

    const initialProject = await Project.upsertUnique({
      value: {
        name: "My project",
      },
      unique: { unique: "First project" },
      owner: publicAccess,
    });
    assertLoaded(initialProject);
    expect(initialProject.name).toEqual("My project");

    const fullProjectList = co
      .list(Project)
      .create([initialProject], publicAccess);

    const account = await createJazzTestAccount({
      isCurrentActiveAccount: true,
    });

    const shallowProjectList = await co
      .list(Project)
      .load(fullProjectList.$jazz.id, {
        loadAs: account,
      });
    assertLoaded(shallowProjectList);

    const publicAccessAsNewAccount = await Group.load(publicAccess.$jazz.id, {
      loadAs: account,
    });
    assertLoaded(publicAccessAsNewAccount);

    const updatedOrg = await Organisation.upsertUnique({
      value: {
        name: "My organisation",
        projects: shallowProjectList,
      },
      unique: { name: "My organisation" },
      owner: publicAccessAsNewAccount,
      resolve: {
        projects: {
          $each: true,
        },
      },
    });

    assertLoaded(updatedOrg);

    expect(updatedOrg.projects.$jazz.id).toEqual(fullProjectList.$jazz.id);
    expect(updatedOrg.projects.length).toBe(1);
    expect(updatedOrg.projects.at(0)?.name).toEqual("My project");
  });

  test("upserting a partially loaded value on an existing value with resolve", async () => {
    const Project = co.map({
      name: z.string(),
    });
    const Organisation = co.map({
      name: z.string(),
      projects: co.list(Project),
    });
    const publicAccess = Group.create();
    publicAccess.addMember("everyone", "writer");

    const initialProject = await Project.upsertUnique({
      value: {
        name: "My project",
      },
      unique: { unique: "First project" },
      owner: publicAccess,
    });
    assertLoaded(initialProject);

    const myOrg = await Organisation.upsertUnique({
      value: {
        name: "My organisation",
        projects: co.list(Project).create([], publicAccess),
      },
      unique: { name: "My organisation" },
      owner: publicAccess,
      resolve: {
        projects: {
          $each: true,
        },
      },
    });
    assertLoaded(myOrg);

    const fullProjectList = co
      .list(Project)
      .create([initialProject], publicAccess);

    const account = await createJazzTestAccount({
      isCurrentActiveAccount: true,
    });

    const shallowProjectList = await co
      .list(Project)
      .load(fullProjectList.$jazz.id, {
        loadAs: account,
      });
    assertLoaded(shallowProjectList);

    const publicAccessAsNewAccount = await Group.load(publicAccess.$jazz.id, {
      loadAs: account,
    });
    assertLoaded(publicAccessAsNewAccount);

    const updatedOrg = await Organisation.upsertUnique({
      value: {
        name: "My organisation",
        projects: shallowProjectList,
      },
      unique: { name: "My organisation" },
      owner: publicAccessAsNewAccount,
      resolve: {
        projects: {
          $each: true,
        },
      },
    });

    assertLoaded(updatedOrg);

    expect(updatedOrg.projects.$jazz.id).toEqual(fullProjectList.$jazz.id);
    expect(updatedOrg.projects.length).toBe(1);
    expect(updatedOrg.projects.at(0)?.name).toEqual("My project");
    expect(updatedOrg.$jazz.id).toEqual(myOrg.$jazz.id);
  });

  test("concurrently upserting the same value", async () => {
    const Project = co.map({
      name: z.string(),
    });

    const owner = Group.create();

    const promises = Array.from({ length: 3 }, (_, i) =>
      Project.upsertUnique({
        owner,
        unique: "concurrent",
        value: { name: `Project ${i}` },
      }),
    );

    await Promise.all(promises);

    const result = await Project.loadUnique("concurrent", owner.$jazz.id);
    assertLoaded(result);

    expect(result.name).toBe(`Project 2`);
  });
});
