import { afterEach, describe, expect, it } from "vitest";
import { createDb, type Db, type QueryBuilder } from "../../src/runtime/db.js";
import { schema as s } from "../../src/index.js";
import { mergePermissionsIntoWasmSchema } from "../../src/schema-permissions.js";
import {
  fetchPermissionsHead,
  publishStoredPermissions,
  publishStoredSchema,
} from "../../src/runtime/schema-fetch.js";
import { getTestingServerInfo, getTestingServerJwtForUser } from "./testing-server.js";
import { TestCleanup, uniqueDbName, withTimeout } from "./support.js";

const snakeCaseSchema = {
  branches: s.table({
    name: s.string(),
    owner_id: s.string(),
  }),
  todos: s.table({
    title: s.string(),
    owner_id: s.string(),
  }),
};
const snakeCaseApp = s.defineApp(snakeCaseSchema);
const branchQuerySchema = {
  projects: s.table({
    name: s.string(),
  }),
  branches: s.table({
    projectId: s.ref("projects"),
    name: s.string(),
    ownerId: s.string(),
  }),
  todos: s.table({
    projectId: s.ref("projects"),
    title: s.string(),
    ownerId: s.string(),
  }),
};
const branchQueryApp = s.defineApp(branchQuerySchema);

type Actor = "alice" | "bob" | "carol";
type TodoRow = s.RowOf<typeof snakeCaseApp.todos>;
type BranchRow = s.RowOf<typeof snakeCaseApp.branches>;
type SimplePolicyMode = "always" | "owner" | "never";
type ReadPolicyMode = SimplePolicyMode | "missing";
type UpdatePolicyMode =
  | "always"
  | "never"
  | "oldOwner"
  | "newOwner"
  | "oldAndNewOwner"
  | "oldOwnerNewAlways"
  | "oldAlwaysNewOwner";

const actors = ["alice", "bob", "carol"] as const satisfies readonly Actor[];
const simplePolicyModes = [
  "always",
  "owner",
  "never",
] as const satisfies readonly SimplePolicyMode[];
const updatePolicyModes = [
  "always",
  "never",
  "oldOwner",
  "newOwner",
  "oldAndNewOwner",
  "oldOwnerNewAlways",
  "oldAlwaysNewOwner",
] as const satisfies readonly UpdatePolicyMode[];

type SimpleAction = "read" | "insert" | "delete";
type CrudMatrixCase =
  | {
      actionUnderTest: SimpleAction;
      normalMode: SimplePolicyMode;
      branchMode: SimplePolicyMode;
    }
  | {
      actionUnderTest: "update";
      normalMode: UpdatePolicyMode;
      branchMode: UpdatePolicyMode;
    };

type SimpleActionBuilder = {
  always(): unknown;
  never(): unknown;
  where(input: Record<string, unknown>): unknown;
};

type UpdateActionBuilder = {
  always(): unknown;
  never(): unknown;
  whereOld(input: Record<string, unknown>): UpdateActionBuilder;
  whereNew(input: Record<string, unknown>): UpdateActionBuilder;
};

type MutationHandle<T = unknown> = {
  batchId?: string;
  wait(options: { tier: "local" | "edge" }): Promise<T>;
};

const readMatrixCases = createSimpleMatrixCases("read");
const insertMatrixCases = createSimpleMatrixCases("insert");
const deleteMatrixCases = createSimpleMatrixCases("delete");
const updateMatrixCases = updatePolicyModes.flatMap((normalMode) =>
  updatePolicyModes.map((branchMode) => ({
    actionUnderTest: "update" as const,
    normalMode,
    branchMode,
  })),
);
const simpleMutationScenarios = [
  { name: "owner actor", actor: "alice", owner: "alice" },
  { name: "non-owner actor", actor: "alice", owner: "bob" },
] as const satisfies readonly {
  name: string;
  actor: Actor;
  owner: Actor;
}[];
const updateMutationScenarios = [
  {
    name: "owner-preserving",
    actor: "alice",
    oldOwner: "alice",
    newOwner: "alice",
  },
  {
    name: "owner-changing-away",
    actor: "alice",
    oldOwner: "alice",
    newOwner: "bob",
  },
  {
    name: "owner-changing-to-actor",
    actor: "alice",
    oldOwner: "bob",
    newOwner: "alice",
  },
  {
    name: "non-owner-preserving",
    actor: "alice",
    oldOwner: "bob",
    newOwner: "bob",
  },
] as const satisfies readonly {
  name: string;
  actor: Actor;
  oldOwner: Actor;
  newOwner: Actor;
}[];

const ctx = new TestCleanup();

afterEach(async () => {
  await ctx.cleanup();
});

function session(userId: string) {
  return {
    user_id: userId,
    claims: {},
    authMode: "external" as const,
  };
}

function createAppWithPermissions(config: CrudMatrixCase): typeof snakeCaseApp {
  const app = s.defineApp(snakeCaseSchema);
  const wasmSchema = mergePermissionsIntoWasmSchema(
    app.wasmSchema,
    defineSnakeCaseCrudPermissions(config),
  );

  (app as unknown as { wasmSchema: typeof wasmSchema }).wasmSchema = wasmSchema;
  (app.branches as unknown as { _schema: typeof wasmSchema })._schema = wasmSchema;
  (app.todos as unknown as { _schema: typeof wasmSchema })._schema = wasmSchema;

  return app;
}

type BranchQueryProjectRow = s.RowOf<typeof branchQueryApp.projects>;
type BranchQueryBranchRow = s.RowOf<typeof branchQueryApp.branches>;
type BranchQueryTodoRow = s.RowOf<typeof branchQueryApp.todos>;

type BranchQueryReadPolicyConfig = {
  normalReadMode: ReadPolicyMode;
  branchReadMode: ReadPolicyMode;
  branchBackingReadMode?: ReadPolicyMode;
  includeForBranchBlock?: boolean;
  includeBranchTodoRead?: boolean;
};

const readPolicyModes = [
  "always",
  "owner",
  "never",
  "missing",
] as const satisfies readonly ReadPolicyMode[];
const branchQueryReadCases = readPolicyModes.flatMap((normalReadMode) =>
  readPolicyModes.map((branchReadMode) => ({
    normalReadMode,
    branchReadMode,
  })),
);

function createSimpleMatrixCases(actionUnderTest: SimpleAction) {
  return simplePolicyModes.flatMap((normalMode) =>
    simplePolicyModes.map((branchMode) => ({
      actionUnderTest,
      normalMode,
      branchMode,
    })),
  );
}

function applySimplePolicy(
  builder: SimpleActionBuilder,
  mode: SimplePolicyMode,
  sessionUserId: unknown,
) {
  switch (mode) {
    case "always":
      builder.always();
      break;
    case "owner":
      builder.where({ owner_id: sessionUserId });
      break;
    case "never":
      builder.never();
      break;
  }
}

function applyReadPolicy(
  builder: SimpleActionBuilder,
  mode: ReadPolicyMode,
  sessionUserId: unknown,
) {
  if (mode === "missing") {
    return;
  }
  applySimplePolicy(builder, mode, sessionUserId);
}

function applyReadPolicyForOwnerColumn(
  builder: SimpleActionBuilder,
  mode: ReadPolicyMode,
  ownerColumn: "owner_id" | "ownerId",
  sessionUserId: unknown,
) {
  switch (mode) {
    case "always":
      builder.always();
      break;
    case "owner":
      builder.where({ [ownerColumn]: sessionUserId });
      break;
    case "never":
      builder.never();
      break;
    case "missing":
      break;
  }
}

function applyUpdatePolicy(
  builder: UpdateActionBuilder,
  mode: UpdatePolicyMode,
  sessionUserId: unknown,
) {
  const ownerCondition = { owner_id: sessionUserId };
  const alwaysCondition = {};
  switch (mode) {
    case "always":
      builder.always();
      break;
    case "never":
      builder.never();
      break;
    case "oldOwner":
      builder.whereOld(ownerCondition);
      break;
    case "newOwner":
      builder.whereNew(ownerCondition);
      break;
    case "oldAndNewOwner":
      builder.whereOld(ownerCondition).whereNew(ownerCondition);
      break;
    case "oldOwnerNewAlways":
      builder.whereOld(ownerCondition).whereNew(alwaysCondition);
      break;
    case "oldAlwaysNewOwner":
      builder.whereOld(alwaysCondition).whereNew(ownerCondition);
      break;
  }
}

function defineSnakeCaseCrudPermissions(config: CrudMatrixCase) {
  return s.definePermissions(snakeCaseApp, ({ policy, session }) => {
    policy.branches.allowRead.always();
    policy.branches.allowInsert.where({ owner_id: session.user_id });
    policy.branches.allowUpdate
      .whereOld({ owner_id: session.user_id })
      .whereNew({ owner_id: session.user_id });
    policy.branches.allowDelete.where({ owner_id: session.user_id });

    applySimplePolicy(
      policy.todos.allowRead,
      config.actionUnderTest === "read" ? config.normalMode : "always",
      session.user_id,
    );
    applySimplePolicy(
      policy.todos.allowInsert,
      config.actionUnderTest === "insert" ? config.normalMode : "always",
      session.user_id,
    );
    applyUpdatePolicy(
      policy.todos.allowUpdate,
      config.actionUnderTest === "update" ? config.normalMode : "always",
      session.user_id,
    );
    applySimplePolicy(
      policy.todos.allowDelete,
      config.actionUnderTest === "delete" ? config.normalMode : "always",
      session.user_id,
    );

    policy.forBranch(policy.branches, ({ branchPolicy, $branch: _branch }) => {
      applySimplePolicy(
        branchPolicy.todos.allowRead,
        config.actionUnderTest === "read" ? config.branchMode : "always",
        session.user_id,
      );
      applySimplePolicy(
        branchPolicy.todos.allowInsert,
        config.actionUnderTest === "insert" ? config.branchMode : "always",
        session.user_id,
      );
      applyUpdatePolicy(
        branchPolicy.todos.allowUpdate,
        config.actionUnderTest === "update" ? config.branchMode : "always",
        session.user_id,
      );
      applySimplePolicy(
        branchPolicy.todos.allowDelete,
        config.actionUnderTest === "delete" ? config.branchMode : "always",
        session.user_id,
      );
    });
  });
}

function defineBranchQueryPermissions(
  app: typeof branchQueryApp,
  config: BranchQueryReadPolicyConfig,
) {
  return s.definePermissions(app, ({ policy, session }) => {
    policy.projects.allowRead.always();
    policy.projects.allowInsert.always();
    policy.projects.allowUpdate.always();
    policy.projects.allowDelete.always();

    applyReadPolicyForOwnerColumn(
      policy.branches.allowRead,
      config.branchBackingReadMode ?? "always",
      "ownerId",
      session.user_id,
    );
    policy.branches.allowInsert.always();
    policy.branches.allowUpdate.always();
    policy.branches.allowDelete.always();

    applyReadPolicyForOwnerColumn(
      policy.todos.allowRead,
      config.normalReadMode,
      "ownerId",
      session.user_id,
    );
    policy.todos.allowInsert.always();
    policy.todos.allowUpdate.always();
    policy.todos.allowDelete.always();

    if (config.includeForBranchBlock !== false) {
      policy.forBranch(policy.branches, ({ branchPolicy }) => {
        if (config.includeBranchTodoRead !== false) {
          applyReadPolicyForOwnerColumn(
            branchPolicy.todos.allowRead,
            config.branchReadMode,
            "ownerId",
            session.user_id,
          );
        }
        branchPolicy.todos.allowInsert.always();
        branchPolicy.todos.allowUpdate.always();
        branchPolicy.todos.allowDelete.always();
      });
    }
  });
}

function simplePolicyAllows(mode: SimplePolicyMode, actor: Actor, owner: Actor) {
  switch (mode) {
    case "always":
      return true;
    case "owner":
      return actor === owner;
    case "never":
      return false;
  }
}

function updatePolicyAllows(
  mode: UpdatePolicyMode,
  actor: Actor,
  oldOwner: Actor,
  newOwner: Actor,
) {
  const oldMatches = actor === oldOwner;
  const newMatches = actor === newOwner;
  switch (mode) {
    case "always":
      return true;
    case "never":
      return false;
    case "oldOwner":
    case "newOwner":
    case "oldAndNewOwner":
      return oldMatches && newMatches;
    case "oldOwnerNewAlways":
      return oldMatches;
    case "oldAlwaysNewOwner":
      return newMatches;
  }
}

async function createAdminDb(appId: string, dbName: string): Promise<Db> {
  return ctx.track(
    await createDb({
      appId,
      env: "test",
      adminSecret: "branch-permissions-admin",
      driver: { type: "persistent", dbName },
    }),
  );
}

async function createActorDb(appId: string, dbName: string, actor: Actor): Promise<Db> {
  return ctx.track(
    await createDb({
      appId,
      env: "test",
      cookieSession: session(actor),
      driver: { type: "persistent", dbName },
    }),
  );
}

async function createCaseDbs(config: CrudMatrixCase, label: string) {
  const app = createAppWithPermissions(config);
  const appId = uniqueDbName(`branch-permissions-${label}`);
  const dbName = uniqueDbName(`branch-permissions-data-${label}`);
  const adminDb = await createAdminDb(appId, dbName);
  return {
    app,
    adminDb,
    actorDb: (actor: Actor) => createActorDb(appId, dbName, actor),
  };
}

async function publishBranchQueryPermissions(label: string, config: BranchQueryReadPolicyConfig) {
  const testingServer = await getTestingServerInfo(uniqueDbName(`branch-query-${label}`));
  const { appId, serverUrl, adminSecret } = testingServer;
  const structuralApp = s.defineApp(branchQuerySchema);
  const permissions = defineBranchQueryPermissions(structuralApp, config);
  const { hash: schemaHash } = await publishStoredSchema(serverUrl, {
    appId,
    adminSecret,
    schema: structuralApp.wasmSchema,
  });
  const { head } = await fetchPermissionsHead(serverUrl, { appId, adminSecret });
  await publishStoredPermissions(serverUrl, {
    appId,
    adminSecret,
    schemaHash,
    permissions,
    expectedParentBundleObjectId: head?.bundleObjectId ?? null,
  });

  return {
    app: structuralApp,
    testingServer,
  };
}

async function createSyncedBranchQueryActorDb(
  testingServer: Awaited<ReturnType<typeof getTestingServerInfo>>,
  label: string,
  actor: Actor,
): Promise<Db> {
  const jwtToken = await getTestingServerJwtForUser(actor, {}, testingServer.appId);
  return ctx.track(
    await createDb({
      appId: testingServer.appId,
      serverUrl: testingServer.serverUrl,
      jwtToken,
      driver: { type: "persistent", dbName: uniqueDbName(label) },
    }),
  );
}

async function createSyncedBranchQueryAdminDb(
  testingServer: Awaited<ReturnType<typeof getTestingServerInfo>>,
  label: string,
): Promise<Db> {
  return ctx.track(
    await createDb({
      appId: testingServer.appId,
      serverUrl: testingServer.serverUrl,
      adminSecret: testingServer.adminSecret,
      driver: { type: "persistent", dbName: uniqueDbName(label) },
    }),
  );
}

async function createSyncedBranchQueryCaseDbs(config: BranchQueryReadPolicyConfig, label: string) {
  const { app, testingServer } = await publishBranchQueryPermissions(label, config);
  return {
    app,
    seedDb: await createSyncedBranchQueryActorDb(testingServer, `${label}-seed`, "alice"),
    actorDb: (actor: Actor) =>
      createSyncedBranchQueryActorDb(testingServer, `${label}-${actor}`, actor),
  };
}

function expectedReadIdsForMode(
  mode: ReadPolicyMode,
  rows: readonly { id: string; owner: Actor }[],
  actor: Actor,
) {
  switch (mode) {
    case "always":
      return rows.map((row) => row.id);
    case "owner":
      return rows.filter((row) => row.owner === actor).map((row) => row.id);
    case "never":
    case "missing":
      return [];
  }
}

async function closeTrackedDb(db: Db) {
  ctx.untrack(db);
  await db.shutdown();
}

function expectedTodo(row: { id: string }, title: string, owner: Actor) {
  return [
    expect.objectContaining({
      id: row.id,
      title,
      owner_id: owner,
    }),
  ];
}

async function expectRows<T>(
  db: Db,
  query: QueryBuilder<T>,
  expectedRows: unknown[],
  label: string,
) {
  const rows = await withTimeout(
    db.all(query, { tier: "local" }),
    15_000,
    `${label}: query did not resolve`,
  );
  expect(rows, label).toEqual(expectedRows);
}

async function expectRowIds<T extends { id: string }>(
  db: Db,
  query: QueryBuilder<T>,
  expectedIds: string[],
  label: string,
  tier: "local" | "edge" = "local",
) {
  const rows = await withTimeout(
    db.all(query, { tier }),
    15_000,
    `${label}: query did not resolve`,
  );
  expect(rows.map((row) => row.id).sort(), label).toEqual([...expectedIds].sort());
}

async function expectRowTitlesInOrder<T extends { title: string }>(
  db: Db,
  query: QueryBuilder<T>,
  expectedTitles: string[],
  label: string,
  tier: "local" | "edge" = "local",
) {
  const rows = await withTimeout(
    db.all(query, { tier }),
    15_000,
    `${label}: query did not resolve`,
  );
  expect(
    rows.map((row) => row.title),
    label,
  ).toEqual(expectedTitles);
}

async function expectMutationAllowed<T>(
  label: string,
  mutate: () => MutationHandle<T>,
  tier: "local" | "edge" = "local",
): Promise<T> {
  try {
    const mutation = mutate();
    return await withTimeout(mutation.wait({ tier }), 20_000, `${label}: mutation did not settle`);
  } catch (error) {
    throw new Error(`${label}: expected mutation to be allowed, got ${String(error)}`);
  }
}

async function expectMutationDenied(label: string, mutate: () => MutationHandle<unknown>) {
  let mutation: MutationHandle<unknown>;
  try {
    mutation = mutate();
  } catch (error) {
    expect(String(error), label).toMatch(/permission_denied|policy denied/i);
    return;
  }

  try {
    await withTimeout(
      mutation.wait({ tier: "local" }),
      20_000,
      `${label}: mutation did not settle`,
    );
  } catch (error) {
    expect(String(error), label).toMatch(/permission_denied|policy denied/i);
    return;
  }

  throw new Error(`${label}: expected mutation to be denied`);
}

async function seedMainTodo(
  app: typeof snakeCaseApp,
  adminDb: Db,
  owner: Actor,
  title: string,
): Promise<TodoRow> {
  return await expectMutationAllowed(`seed main todo ${title}`, () =>
    adminDb.insert(app.todos, {
      title,
      owner_id: owner,
    }),
  );
}

async function seedBranch(
  app: typeof snakeCaseApp,
  adminDb: Db,
  owner: Actor,
  name: string,
): Promise<BranchRow> {
  return await expectMutationAllowed(`seed branch ${name}`, () =>
    adminDb.insert(app.branches, {
      name,
      owner_id: owner,
    }),
  );
}

async function seedBranchTodo(
  app: typeof snakeCaseApp,
  adminDb: Db,
  branchId: string,
  owner: Actor,
  title: string,
): Promise<TodoRow> {
  return await expectMutationAllowed(`seed branch todo ${title}`, () =>
    adminDb.branch(branchId).insert(app.todos, {
      title,
      owner_id: owner,
    }),
  );
}

async function seedBranchQueryProject(
  app: typeof branchQueryApp,
  adminDb: Db,
  name: string,
  tier: "local" | "edge" = "local",
): Promise<BranchQueryProjectRow> {
  return await expectMutationAllowed(
    `seed branch query project ${name}`,
    () =>
      adminDb.insert(app.projects, {
        name,
      }),
    tier,
  );
}

async function seedBranchQueryBranch(
  app: typeof branchQueryApp,
  adminDb: Db,
  projectId: string,
  owner: Actor,
  name: string,
  tier: "local" | "edge" = "local",
): Promise<BranchQueryBranchRow> {
  return await expectMutationAllowed(
    `seed branch query branch ${name}`,
    () =>
      adminDb.insert(app.branches, {
        projectId,
        name,
        ownerId: owner,
      }),
    tier,
  );
}

async function seedBranchQueryMainTodo(
  app: typeof branchQueryApp,
  adminDb: Db,
  projectId: string,
  owner: Actor,
  title: string,
  tier: "local" | "edge" = "local",
): Promise<BranchQueryTodoRow> {
  return await expectMutationAllowed(
    `seed branch query main todo ${title}`,
    () =>
      adminDb.insert(app.todos, {
        projectId,
        title,
        ownerId: owner,
      }),
    tier,
  );
}

async function seedBranchQueryBranchTodo(
  app: typeof branchQueryApp,
  adminDb: Db,
  branchId: string,
  projectId: string,
  owner: Actor,
  title: string,
  tier: "local" | "edge" = "local",
): Promise<BranchQueryTodoRow> {
  return await expectMutationAllowed(
    `seed branch query branch todo ${title}`,
    () =>
      adminDb.branch(branchId).insert(app.todos, {
        projectId,
        title,
        ownerId: owner,
      }),
    tier,
  );
}

describe("branch permissions browser integration", () => {
  describe.each(readMatrixCases)(
    "read matrix: normal=$normalMode branch=$branchMode",
    ({ normalMode, branchMode }) => {
      /*
       * Each generated read case checks the policy split:
       *
       *   main storage                 branch storage
       *   todos(main: alice)           branches(alice) -> todos(branch: bob)
       *        | normal read policy          | branch read policy
       *        v                             v
       *      reader                       reader.branch(branch_id)
       *
       *   Surface                              Expected policy
       *   -----------------------------------  ----------------
       *   db.all(todos where main title)       normal read
       *   db.all(todos where branch title)     normal read, no branch leak
       *   db.branch(id).all(todos)             branch read
       *   db.all(todos.branch(id))             branch read
       */
      it("applies normal read policy on main and branch read policy on branches", async () => {
        const caseName = `read normal=${normalMode} branch=${branchMode}`;
        const { app, adminDb, actorDb } = await createCaseDbs(
          {
            actionUnderTest: "read",
            normalMode,
            branchMode,
          },
          caseName,
        );
        const mainTitle = `Main todo ${caseName}`;
        const branchTitle = `Branch todo ${caseName}`;
        const mainOwner = "alice";
        const branchTodoOwner = "bob";
        const mainTodo = await seedMainTodo(app, adminDb, mainOwner, mainTitle);
        const branch = await seedBranch(app, adminDb, "alice", `Draft ${caseName}`);
        const branchTodo = await seedBranchTodo(
          app,
          adminDb,
          branch.id,
          branchTodoOwner,
          branchTitle,
        );

        await expectRows(
          adminDb,
          app.todos.where({ title: mainTitle }),
          expectedTodo(mainTodo, mainTitle, mainOwner),
          `${caseName} admin sees main seed`,
        );
        await expectRows(
          adminDb.branch(branch.id),
          app.todos.where({ title: branchTitle }),
          expectedTodo(branchTodo, branchTitle, branchTodoOwner),
          `${caseName} admin sees branch seed`,
        );
        await closeTrackedDb(adminDb);

        for (const reader of actors) {
          const readerDb = await actorDb(reader);
          const readerCase = `${caseName} reader=${reader}`;
          const expectedMain = simplePolicyAllows(normalMode, reader, mainOwner)
            ? expectedTodo(mainTodo, mainTitle, mainOwner)
            : [];
          const expectedBranch = simplePolicyAllows(branchMode, reader, branchTodoOwner)
            ? expectedTodo(branchTodo, branchTitle, branchTodoOwner)
            : [];

          await expectRows(
            readerDb,
            app.todos.where({ title: mainTitle }),
            expectedMain,
            `${readerCase} main title`,
          );
          await expectRows(
            readerDb,
            app.todos.where({ title: branchTitle }),
            [],
            `${readerCase} branch title on main`,
          );
          await expectRows(readerDb, app.todos.where({}), expectedMain, `${readerCase} all main`);
          await expectRows(
            readerDb,
            app.branches.where({ id: branch.id }),
            [
              expect.objectContaining({
                id: branch.id,
                name: `Draft ${caseName}`,
                owner_id: "alice",
              }),
            ],
            `${readerCase} branch backing row`,
          );
          await expectRows(
            readerDb.branch(branch.id),
            app.todos.where({ title: branchTitle }),
            expectedBranch,
            `${readerCase} branch db`,
          );
          await expectRows(
            readerDb,
            app.todos.where({ title: branchTitle }).branch(branch.id),
            expectedBranch,
            `${readerCase} query branch`,
          );
          await expectRows(
            readerDb.branch(branch.id),
            app.todos.where({ title: branchTitle }).branch(branch.id),
            expectedBranch,
            `${readerCase} branch db with query branch`,
          );
        }
      });
    },
  );

  describe.each(insertMatrixCases)(
    "insert matrix: normal=$normalMode branch=$branchMode",
    ({ normalMode, branchMode }) => {
      /*
       * Each generated insert case targets exactly one branch family:
       *
       *   Write target                         Policy used
       *   -----------------------------------  ----------------
       *   writerDb.insert(todos)               normal insert
       *   writerDb.branch(id).insert(todos)    branch insert
       *
       *   Actor/owner rows:
       *   alice -> owner alice  => owner policy allows
       *   alice -> owner bob    => owner policy denies
       */
      it("applies insert policy for the targeted branch only", async () => {
        const caseName = `insert normal=${normalMode} branch=${branchMode}`;
        const { app, adminDb, actorDb } = await createCaseDbs(
          {
            actionUnderTest: "insert",
            normalMode,
            branchMode,
          },
          caseName,
        );
        const branch = await seedBranch(app, adminDb, "alice", `Draft ${caseName}`);
        await closeTrackedDb(adminDb);
        const writerDb = await actorDb("alice");
        await expectRows(
          writerDb,
          app.branches.where({ id: branch.id }),
          [
            expect.objectContaining({
              id: branch.id,
              name: `Draft ${caseName}`,
              owner_id: "alice",
            }),
          ],
          `${caseName} writer sees branch backing row`,
        );

        for (const scenario of simpleMutationScenarios) {
          const mainTitle = `Main ${caseName} ${scenario.name}`;
          const branchTitle = `Branch ${caseName} ${scenario.name}`;
          const mainAllowed = simplePolicyAllows(normalMode, scenario.actor, scenario.owner);
          const branchAllowed = simplePolicyAllows(branchMode, scenario.actor, scenario.owner);

          if (mainAllowed) {
            const row = await expectMutationAllowed(`${caseName} main ${scenario.name}`, () =>
              writerDb.insert(app.todos, {
                title: mainTitle,
                owner_id: scenario.owner,
              }),
            );
            await expectRows(
              writerDb,
              app.todos.where({ title: mainTitle }),
              expectedTodo(row, mainTitle, scenario.owner),
              `${caseName} main inserted row survives ${scenario.name}`,
            );
          } else {
            await expectMutationDenied(`${caseName} main ${scenario.name}`, () =>
              writerDb.insert(app.todos, {
                title: mainTitle,
                owner_id: scenario.owner,
              }),
            );
            await expectRows(
              writerDb,
              app.todos.where({ title: mainTitle }),
              [],
              `${caseName} main denied row absent ${scenario.name}`,
            );
          }

          if (branchAllowed) {
            const row = await expectMutationAllowed(`${caseName} branch ${scenario.name}`, () =>
              writerDb.branch(branch.id).insert(app.todos, {
                title: branchTitle,
                owner_id: scenario.owner,
              }),
            );
            await expectRows(
              writerDb.branch(branch.id),
              app.todos.where({ title: branchTitle }),
              expectedTodo(row, branchTitle, scenario.owner),
              `${caseName} branch inserted row survives ${scenario.name}`,
            );
          } else {
            await expectMutationDenied(`${caseName} branch ${scenario.name}`, () =>
              writerDb.branch(branch.id).insert(app.todos, {
                title: branchTitle,
                owner_id: scenario.owner,
              }),
            );
            await expectRows(
              writerDb.branch(branch.id),
              app.todos.where({ title: branchTitle }),
              [],
              `${caseName} branch denied row absent ${scenario.name}`,
            );
          }
        }
      });
    },
  );

  describe.each(updateMatrixCases)(
    "update matrix: normal=$normalMode branch=$branchMode",
    ({ normalMode, branchMode }) => {
      /*
       * Each generated update case isolates old/new-owner policy clauses:
       *
       *   Existing row owner ----update----> New row owner
       *          |                              |
       *       whereOld                      whereNew
       *
       *   Write target                         Policy used
       *   -----------------------------------  ----------------
       *   writerDb.update(todos, id)           normal update
       *   writerDb.branch(id).update(todos)    branch update
       */
      it("applies update policy for the targeted branch only", async () => {
        const caseName = `update normal=${normalMode} branch=${branchMode}`;
        const { app, adminDb, actorDb } = await createCaseDbs(
          {
            actionUnderTest: "update",
            normalMode,
            branchMode,
          },
          caseName,
        );
        const branch = await seedBranch(app, adminDb, "alice", `Draft ${caseName}`);
        const seededScenarios = [];

        for (const scenario of updateMutationScenarios) {
          const mainTitle = `Main ${caseName} ${scenario.name}`;
          const mainNewTitle = `${mainTitle} updated`;
          const branchTitle = `Branch ${caseName} ${scenario.name}`;
          const branchNewTitle = `${branchTitle} updated`;
          const mainRow = await seedMainTodo(app, adminDb, scenario.oldOwner, mainTitle);
          const branchRow = await seedBranchTodo(
            app,
            adminDb,
            branch.id,
            scenario.oldOwner,
            branchTitle,
          );
          seededScenarios.push({
            scenario,
            mainTitle,
            mainNewTitle,
            branchTitle,
            branchNewTitle,
            mainRow,
            branchRow,
          });
        }

        await closeTrackedDb(adminDb);
        const writerDb = await actorDb("alice");
        await expectRows(
          writerDb,
          app.branches.where({ id: branch.id }),
          [
            expect.objectContaining({
              id: branch.id,
              name: `Draft ${caseName}`,
              owner_id: "alice",
            }),
          ],
          `${caseName} writer sees branch backing row`,
        );

        for (const seeded of seededScenarios) {
          const {
            scenario,
            mainTitle,
            mainNewTitle,
            branchTitle,
            branchNewTitle,
            mainRow,
            branchRow,
          } = seeded;
          await expectRows(
            writerDb,
            app.todos.where({ id: mainRow.id }),
            expectedTodo(mainRow, mainTitle, scenario.oldOwner),
            `${caseName} main row loaded before update ${scenario.name}`,
          );
          await expectRows(
            writerDb.branch(branch.id),
            app.todos.where({ id: branchRow.id }),
            expectedTodo(branchRow, branchTitle, scenario.oldOwner),
            `${caseName} branch row loaded before update ${scenario.name}`,
          );
          const mainAllowed = updatePolicyAllows(
            normalMode,
            scenario.actor,
            scenario.oldOwner,
            scenario.newOwner,
          );
          const branchAllowed = updatePolicyAllows(
            branchMode,
            scenario.actor,
            scenario.oldOwner,
            scenario.newOwner,
          );

          if (mainAllowed) {
            await expectMutationAllowed(`${caseName} main ${scenario.name}`, () =>
              writerDb.update(app.todos, mainRow.id, {
                title: mainNewTitle,
                owner_id: scenario.newOwner,
              }),
            );
            await expectRows(
              writerDb,
              app.todos.where({ id: mainRow.id }),
              expectedTodo(mainRow, mainNewTitle, scenario.newOwner),
              `${caseName} main allowed update persisted ${scenario.name}`,
            );
          } else {
            await expectMutationDenied(`${caseName} main ${scenario.name}`, () =>
              writerDb.update(app.todos, mainRow.id, {
                title: mainNewTitle,
                owner_id: scenario.newOwner,
              }),
            );
            await expectRows(
              writerDb,
              app.todos.where({ id: mainRow.id }),
              expectedTodo(mainRow, mainTitle, scenario.oldOwner),
              `${caseName} main denied update preserved ${scenario.name}`,
            );
          }

          if (branchAllowed) {
            await expectMutationAllowed(`${caseName} branch ${scenario.name}`, () =>
              writerDb.branch(branch.id).update(app.todos, branchRow.id, {
                title: branchNewTitle,
                owner_id: scenario.newOwner,
              }),
            );
            await expectRows(
              writerDb.branch(branch.id),
              app.todos.where({ id: branchRow.id }),
              expectedTodo(branchRow, branchNewTitle, scenario.newOwner),
              `${caseName} branch allowed update persisted ${scenario.name}`,
            );
          } else {
            await expectMutationDenied(`${caseName} branch ${scenario.name}`, () =>
              writerDb.branch(branch.id).update(app.todos, branchRow.id, {
                title: branchNewTitle,
                owner_id: scenario.newOwner,
              }),
            );
            await expectRows(
              writerDb.branch(branch.id),
              app.todos.where({ id: branchRow.id }),
              expectedTodo(branchRow, branchTitle, scenario.oldOwner),
              `${caseName} branch denied update preserved ${scenario.name}`,
            );
          }
        }
      });
    },
  );

  describe.each(deleteMatrixCases)(
    "delete matrix: normal=$normalMode branch=$branchMode",
    ({ normalMode, branchMode }) => {
      /*
       * Each generated delete case proves deletes do not cross branches:
       *
       *   Main row         --delete--> normal delete policy
       *   Branch row       --delete--> branch delete policy
       *
       *   Policy result    Expected row state
       *   ---------------  ------------------
       *   allowed          gone from that branch family
       *   denied           still readable from that branch family
       */
      it("applies delete policy for the targeted branch only", async () => {
        const caseName = `delete normal=${normalMode} branch=${branchMode}`;
        const { app, adminDb, actorDb } = await createCaseDbs(
          {
            actionUnderTest: "delete",
            normalMode,
            branchMode,
          },
          caseName,
        );
        const branch = await seedBranch(app, adminDb, "alice", `Draft ${caseName}`);
        const seededScenarios = [];

        for (const scenario of simpleMutationScenarios) {
          const mainTitle = `Main ${caseName} ${scenario.name}`;
          const branchTitle = `Branch ${caseName} ${scenario.name}`;
          const mainRow = await seedMainTodo(app, adminDb, scenario.owner, mainTitle);
          const branchRow = await seedBranchTodo(
            app,
            adminDb,
            branch.id,
            scenario.owner,
            branchTitle,
          );
          seededScenarios.push({
            scenario,
            mainTitle,
            branchTitle,
            mainRow,
            branchRow,
          });
        }

        await closeTrackedDb(adminDb);
        const writerDb = await actorDb("alice");
        await expectRows(
          writerDb,
          app.branches.where({ id: branch.id }),
          [
            expect.objectContaining({
              id: branch.id,
              name: `Draft ${caseName}`,
              owner_id: "alice",
            }),
          ],
          `${caseName} writer sees branch backing row`,
        );

        for (const seeded of seededScenarios) {
          const { scenario, mainTitle, branchTitle, mainRow, branchRow } = seeded;
          await expectRows(
            writerDb,
            app.todos.where({ id: mainRow.id }),
            expectedTodo(mainRow, mainTitle, scenario.owner),
            `${caseName} main row loaded before delete ${scenario.name}`,
          );
          await expectRows(
            writerDb.branch(branch.id),
            app.todos.where({ id: branchRow.id }),
            expectedTodo(branchRow, branchTitle, scenario.owner),
            `${caseName} branch row loaded before delete ${scenario.name}`,
          );
          const mainAllowed = simplePolicyAllows(normalMode, scenario.actor, scenario.owner);
          const branchAllowed = simplePolicyAllows(branchMode, scenario.actor, scenario.owner);

          if (mainAllowed) {
            await expectMutationAllowed(`${caseName} main ${scenario.name}`, () =>
              writerDb.delete(app.todos, mainRow.id),
            );
            await expectRows(
              writerDb,
              app.todos.where({ id: mainRow.id }),
              [],
              `${caseName} main allowed delete removed row ${scenario.name}`,
            );
          } else {
            await expectMutationDenied(`${caseName} main ${scenario.name}`, () =>
              writerDb.delete(app.todos, mainRow.id),
            );
            await expectRows(
              writerDb,
              app.todos.where({ id: mainRow.id }),
              expectedTodo(mainRow, mainTitle, scenario.owner),
              `${caseName} main denied delete preserved row ${scenario.name}`,
            );
          }

          if (branchAllowed) {
            await expectMutationAllowed(`${caseName} branch ${scenario.name}`, () =>
              writerDb.branch(branch.id).delete(app.todos, branchRow.id),
            );
            await expectRows(
              writerDb.branch(branch.id),
              app.todos.where({ id: branchRow.id }),
              [],
              `${caseName} branch allowed delete removed row ${scenario.name}`,
            );
          } else {
            await expectMutationDenied(`${caseName} branch ${scenario.name}`, () =>
              writerDb.branch(branch.id).delete(app.todos, branchRow.id),
            );
            await expectRows(
              writerDb.branch(branch.id),
              app.todos.where({ id: branchRow.id }),
              expectedTodo(branchRow, branchTitle, scenario.owner),
              `${caseName} branch denied delete preserved row ${scenario.name}`,
            );
          }
        }
      });
    },
  );
});

async function seedBranchQueryFixture(
  app: typeof branchQueryApp,
  adminDb: Db,
  caseName: string,
  tier: "local" | "edge" = "local",
) {
  const project = await seedBranchQueryProject(app, adminDb, `Project ${caseName}`, tier);
  const mainTodo = await seedBranchQueryMainTodo(
    app,
    adminDb,
    project.id,
    "alice",
    `Main todo ${caseName}`,
    tier,
  );
  const otherMainTodo = await seedBranchQueryMainTodo(
    app,
    adminDb,
    project.id,
    "bob",
    `Other main todo ${caseName}`,
    tier,
  );
  const branchA = await seedBranchQueryBranch(
    app,
    adminDb,
    project.id,
    "alice",
    `Draft A ${caseName}`,
    tier,
  );
  const branchB = await seedBranchQueryBranch(
    app,
    adminDb,
    project.id,
    "alice",
    `Draft B ${caseName}`,
    tier,
  );
  const branchATodo = await seedBranchQueryBranchTodo(
    app,
    adminDb,
    branchA.id,
    project.id,
    "bob",
    `Branch A todo ${caseName}`,
    tier,
  );
  const branchBTodo = await seedBranchQueryBranchTodo(
    app,
    adminDb,
    branchB.id,
    project.id,
    "alice",
    `Branch B todo ${caseName}`,
    tier,
  );

  return {
    project,
    mainTodo,
    otherMainTodo,
    branchA,
    branchB,
    branchATodo,
    branchBTodo,
  };
}

describe("branch query permissions browser integration", () => {
  describe.each(branchQueryReadCases)(
    "read surfaces: normal=$normalReadMode branch=$branchReadMode",
    ({ normalReadMode, branchReadMode }) => {
      /*
       * Each generated browser case uses the same fixture:
       *
       *   main branch
       *   project P
       *     |- todo M1 owner alice
       *     `- todo M2 owner bob
       *
       *   branch A backing row owner alice -> todo A owner bob
       *   branch B backing row owner alice -> todo B owner alice
       *
       *   Query surface                         Branch chosen  Policy used
       *   ------------------------------------  -------------  -----------
       *   db.all(todos where project=P)         main           normal read
       *   db.branch(A).all(todos)               A              branch read
       *   db.all(todos.branch(A))               A              branch read
       *   db.branch(A).all(todos.branch(B))     B              branch read
       */
      it("applies normal, branch, and override read policies", async () => {
        const caseName = `normal=${normalReadMode} branch=${branchReadMode}`;
        const { app, seedDb } = await createSyncedBranchQueryCaseDbs(
          {
            normalReadMode,
            branchReadMode,
            branchBackingReadMode: "always",
          },
          caseName,
        );
        const fixture = await seedBranchQueryFixture(app, seedDb, caseName, "edge");
        const reader = "alice";
        const expectedMain = expectedReadIdsForMode(
          normalReadMode,
          [
            { id: fixture.mainTodo.id, owner: "alice" },
            { id: fixture.otherMainTodo.id, owner: "bob" },
          ],
          reader,
        );
        const expectedBranchA = expectedReadIdsForMode(
          branchReadMode,
          [{ id: fixture.branchATodo.id, owner: "bob" }],
          reader,
        );
        const expectedBranchB = expectedReadIdsForMode(
          branchReadMode,
          [{ id: fixture.branchBTodo.id, owner: "alice" }],
          reader,
        );

        await expectRowIds(
          seedDb,
          app.todos.where({ projectId: fixture.project.id }),
          expectedMain,
          `${caseName} main project query uses normal read policy`,
          "edge",
        );
        await expectRowIds(
          seedDb,
          app.todos.where({ title: `Branch A todo ${caseName}` }),
          [],
          `${caseName} branch A row does not leak into main reads`,
          "edge",
        );
        await expectRowIds(
          seedDb.branch(fixture.branchA.id),
          app.todos.where({ projectId: fixture.project.id }),
          expectedBranchA,
          `${caseName} branch db uses branch read policy`,
          "edge",
        );
        await expectRowIds(
          seedDb,
          app.todos.where({ projectId: fixture.project.id }).branch(fixture.branchA.id),
          expectedBranchA,
          `${caseName} query-level branch uses branch read policy`,
          "edge",
        );
        await expectRowIds(
          seedDb.branch(fixture.branchA.id),
          app.todos.where({ projectId: fixture.project.id }).branch(fixture.branchB.id),
          expectedBranchB,
          `${caseName} query-level branch overrides branch db view`,
          "edge",
        );
      });
    },
  );

  /*
   * Include relation path:
   *
   *   project P --include todos.branch(A)--> branch A todos
   *
   *   projects.allowRead = always
   *   branch todos allowRead = never
   *
   *   Expected: [project P { todosViaProject: [] }]
   */
  it("does not leak denied branch rows through included relations", async () => {
    const caseName = "include-denied-branch-read";
    const { app, seedDb } = await createSyncedBranchQueryCaseDbs(
      {
        normalReadMode: "always",
        branchReadMode: "never",
        branchBackingReadMode: "always",
      },
      caseName,
    );
    const fixture = await seedBranchQueryFixture(app, seedDb, caseName, "edge");

    const projectRows = await withTimeout(
      seedDb.all(
        app.projects.where({ id: fixture.project.id }).include({
          todosViaProject: app.todos.branch(fixture.branchA.id),
        }),
        { tier: "edge" },
      ),
      15_000,
      "denied branch include query did not resolve",
    );
    expect(projectRows, "project row remains visible when include is empty").toHaveLength(1);
    expect(projectRows[0]!.todosViaProject).toEqual([]);
  });

  /*
   * Union relation path:
   *
   *   union(
   *     todos.branch(A),
   *     todos.branch(B)
   *   )
   *
   *   Branch read policy = never for both arms.
   *   Expected: no rows from either branch arm.
   */
  it("does not leak denied branch rows through union inputs", async () => {
    const caseName = "union-denied-branch-read";
    const { app, seedDb, actorDb } = await createSyncedBranchQueryCaseDbs(
      {
        normalReadMode: "always",
        branchReadMode: "never",
        branchBackingReadMode: "always",
      },
      caseName,
    );
    const fixture = await seedBranchQueryFixture(app, seedDb, caseName, "edge");
    const readerDb = await actorDb("alice");

    await expectRowIds(
      readerDb,
      app.union([
        app.todos.where({ projectId: fixture.project.id }).branch(fixture.branchA.id),
        app.todos.where({ projectId: fixture.project.id }).branch(fixture.branchB.id),
      ]),
      [],
      "denied branch union inputs stay hidden",
      "edge",
    );
  });

  /*
   * Positive union relation path:
   *
   *   union(
   *     todos.branch(A),
   *     todos.branch(B)
   *   )
   *
   *   Branch read policy = always for both arms.
   *   Expected: rows from both branch arms are returned, and main rows stay out.
   */
  it("returns allowed rows from unioned branch inputs", async () => {
    const caseName = "union-allowed-branch-read";
    const { app, seedDb } = await createSyncedBranchQueryCaseDbs(
      {
        normalReadMode: "always",
        branchReadMode: "always",
        branchBackingReadMode: "always",
      },
      caseName,
    );
    const fixture = await seedBranchQueryFixture(app, seedDb, caseName, "edge");

    await expectRowIds(
      seedDb,
      app.union([
        app.todos.where({ projectId: fixture.project.id }).branch(fixture.branchA.id),
        app.todos.where({ projectId: fixture.project.id }).branch(fixture.branchB.id),
      ]),
      [fixture.branchATodo.id, fixture.branchBTodo.id],
      "allowed branch union inputs return rows from both branches",
      "edge",
    );
  });

  /*
   * Branch-local ordering path:
   *
   *   branch A contains Z and M titles
   *   main contains a lexically larger title
   *
   *   Query: todos.branch(A).orderBy(title desc).limit(2)
   *   Expected: branch-local sorted window only.
   */
  it("applies orderBy and limit within the selected branch", async () => {
    const caseName = "branch-order-limit";
    const { app, seedDb } = await createSyncedBranchQueryCaseDbs(
      {
        normalReadMode: "always",
        branchReadMode: "always",
        branchBackingReadMode: "always",
      },
      caseName,
    );
    const project = await seedBranchQueryProject(app, seedDb, `Project ${caseName}`, "edge");
    await seedBranchQueryMainTodo(app, seedDb, project.id, "alice", "ZZZ main leak", "edge");
    const branch = await seedBranchQueryBranch(
      app,
      seedDb,
      project.id,
      "alice",
      `Draft ${caseName}`,
      "edge",
    );
    await seedBranchQueryBranchTodo(app, seedDb, branch.id, project.id, "alice", "A-low", "edge");
    await seedBranchQueryBranchTodo(app, seedDb, branch.id, project.id, "alice", "Z-top", "edge");
    await seedBranchQueryBranchTodo(app, seedDb, branch.id, project.id, "alice", "M-mid", "edge");

    await expectRowTitlesInOrder(
      seedDb,
      app.todos
        .where({ projectId: project.id })
        .branch(branch.id)
        .orderBy("title", "desc")
        .limit(2),
      ["Z-top", "M-mid"],
      "branch query applies descending order and limit",
      "edge",
    );
  });

  /*
   * Union ordering path:
   *
   *   union(todos.branch(A), todos.branch(B))
   *     .orderBy(title desc)
   *     .limit(2)
   *
   *   Expected: the sorted window is taken across both branch arms.
   */
  it("applies orderBy and limit across unioned branch inputs", async () => {
    const caseName = "branch-union-order-limit";
    const { app, seedDb } = await createSyncedBranchQueryCaseDbs(
      {
        normalReadMode: "always",
        branchReadMode: "always",
        branchBackingReadMode: "always",
      },
      caseName,
    );
    const project = await seedBranchQueryProject(app, seedDb, `Project ${caseName}`, "edge");
    await seedBranchQueryMainTodo(app, seedDb, project.id, "alice", "ZZZ main leak", "edge");
    const branchA = await seedBranchQueryBranch(
      app,
      seedDb,
      project.id,
      "alice",
      `Draft A ${caseName}`,
      "edge",
    );
    const branchB = await seedBranchQueryBranch(
      app,
      seedDb,
      project.id,
      "alice",
      `Draft B ${caseName}`,
      "edge",
    );
    await seedBranchQueryBranchTodo(app, seedDb, branchA.id, project.id, "alice", "A-low", "edge");
    await seedBranchQueryBranchTodo(app, seedDb, branchA.id, project.id, "alice", "Z-top", "edge");
    await seedBranchQueryBranchTodo(app, seedDb, branchB.id, project.id, "alice", "M-mid", "edge");
    await seedBranchQueryBranchTodo(app, seedDb, branchB.id, project.id, "alice", "B-low", "edge");

    await expectRowTitlesInOrder(
      seedDb,
      app
        .union([
          app.todos.where({ projectId: project.id }).branch(branchA.id),
          app.todos.where({ projectId: project.id }).branch(branchB.id),
        ])
        .orderBy("title", "desc")
        .limit(2),
      ["Z-top", "M-mid"],
      "branch union applies descending order and limit across arms",
      "edge",
    );
  });

  /*
   * Missing forBranch block:
   *
   *   normal todos read = always
   *   branches read     = always
   *   forBranch(...)    = absent
   *
   *   db.branch(A).all(todos) must deny by default.
   */
  it("denies branch reads when no forBranch block is defined", async () => {
    const caseName = "missing-for-branch-block";
    const { app, testingServer } = await publishBranchQueryPermissions(caseName, {
      normalReadMode: "always",
      branchReadMode: "always",
      branchBackingReadMode: "always",
      includeForBranchBlock: false,
    });
    const seedDb = await createSyncedBranchQueryAdminDb(testingServer, `${caseName}-seed`);
    const fixture = await seedBranchQueryFixture(app, seedDb, caseName, "edge");
    const readerDb = await createSyncedBranchQueryActorDb(
      testingServer,
      `${caseName}-alice`,
      "alice",
    );

    await expectRowIds(
      readerDb.branch(fixture.branchA.id),
      app.todos.where({ projectId: fixture.project.id }),
      [],
      "branch reads deny when forBranch is absent",
      "edge",
    );
  });

  /*
   * Missing table clause inside forBranch:
   *
   *   policy.forBranch(branches, ({ branchPolicy }) => {
   *     // branchPolicy.todos.allowRead is not called
   *   })
   *
   *   Branch-specific reads must fail closed, not fall back to normal read.
   */
  it("denies branch reads when forBranch omits the target table read clause", async () => {
    const caseName = "missing-branch-todo-read";
    const { app, testingServer } = await publishBranchQueryPermissions(caseName, {
      normalReadMode: "always",
      branchReadMode: "always",
      branchBackingReadMode: "always",
      includeBranchTodoRead: false,
    });
    const seedDb = await createSyncedBranchQueryAdminDb(testingServer, `${caseName}-seed`);
    const fixture = await seedBranchQueryFixture(app, seedDb, caseName, "edge");
    const readerDb = await createSyncedBranchQueryActorDb(
      testingServer,
      `${caseName}-alice`,
      "alice",
    );

    await expectRowIds(
      readerDb.branch(fixture.branchA.id),
      app.todos.where({ projectId: fixture.project.id }),
      [],
      "branch reads deny when branch todo read is omitted",
      "edge",
    );
  });

  describe.each(["missing", "never"] as const satisfies readonly ReadPolicyMode[])(
    "backing branch read mode $0",
    (branchBackingReadMode) => {
      /*
       * Backing-row gate:
       *
       *   db.branch(A).all(todos)
       *          |
       *          +-- branches[A].allowRead must pass
       *          `-- branchPolicy.todos.allowRead must pass
       *
       *   This generated case varies the first gate as missing/never.
       */
      it("denies branch reads when the backing row is not readable", async () => {
        const caseName = `backing=${branchBackingReadMode}`;
        const { app, testingServer } = await publishBranchQueryPermissions(caseName, {
          normalReadMode: "always",
          branchReadMode: "always",
          branchBackingReadMode,
        });
        const seedDb = await createSyncedBranchQueryAdminDb(testingServer, `${caseName}-seed`);
        const fixture = await seedBranchQueryFixture(app, seedDb, caseName, "edge");
        const readerDb = await createSyncedBranchQueryActorDb(
          testingServer,
          `${caseName}-alice`,
          "alice",
        );

        await expectRowIds(
          readerDb.branch(fixture.branchA.id),
          app.todos.where({ projectId: fixture.project.id }),
          [],
          `${caseName} branch read requires readable backing row`,
          "edge",
        );
      });
    },
  );

  describe.each([
    { name: "never", branchReadMode: "never" as const },
    { name: "missing", branchReadMode: "missing" as const },
  ])("synced branch read mode $name", ({ name, branchReadMode }) => {
    /*
     * Published-permissions server path:
     *
     *   browser DB -> edge query -> structural schema graph
     *                         `-> published permissions head
     *
     *   normal read = always, branch read = never/missing.
     *   Expected: edge branch query and include both return no todo rows.
     */
    it("uses published branch read permissions for edge branch queries", async () => {
      const { app, testingServer } = await publishBranchQueryPermissions(`synced-${name}`, {
        normalReadMode: "always",
        branchReadMode,
        branchBackingReadMode: "always",
      });
      const db = await createSyncedBranchQueryActorDb(
        testingServer,
        `synced-branch-query-${name}`,
        "alice",
      );
      const project = await seedBranchQueryProject(app, db, `Synced project ${name}`, "edge");
      const branch = await seedBranchQueryBranch(
        app,
        db,
        project.id,
        "alice",
        `Synced branch ${name}`,
        "edge",
      );

      await seedBranchQueryBranchTodo(
        app,
        db,
        branch.id,
        project.id,
        "alice",
        `Synced hidden todo ${name}`,
        "edge",
      );

      await expectRowIds(
        db.branch(branch.id),
        app.todos.where({ projectId: project.id }),
        [],
        `synced ${name} edge branch read`,
        "edge",
      );

      const projectRows = await withTimeout(
        db.all(
          app.projects.where({ id: project.id }).include({
            todosViaProject: app.todos.branch(branch.id),
          }),
          { tier: "edge" },
        ),
        15_000,
        `synced ${name} include query did not resolve`,
      );
      expect(projectRows, `synced ${name} project row visible for include`).toHaveLength(1);
      expect(projectRows[0]!.todosViaProject).toEqual([]);
    });
  });
});
