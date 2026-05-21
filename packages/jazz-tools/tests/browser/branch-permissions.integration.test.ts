import { afterEach, describe, expect, it } from "vitest";
import { createDb, type Db, type QueryBuilder } from "../../src/runtime/db.js";
import { schema as s } from "../../src/index.js";
import { mergePermissionsIntoWasmSchema } from "../../src/schema-permissions.js";
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

type Actor = "alice" | "bob" | "carol";
type TodoRow = s.RowOf<typeof snakeCaseApp.todos>;
type BranchRow = s.RowOf<typeof snakeCaseApp.branches>;
type SimplePolicyMode = "always" | "owner" | "never";
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
  wait(options: { tier: "local" }): Promise<T>;
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

async function expectMutationAllowed<T>(
  label: string,
  mutate: () => MutationHandle<T>,
): Promise<T> {
  try {
    const mutation = mutate();
    return await withTimeout(
      mutation.wait({ tier: "local" }),
      20_000,
      `${label}: mutation did not settle`,
    );
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

describe("branch permissions browser integration", () => {
  describe.each(readMatrixCases)(
    "read matrix: normal=$normalMode branch=$branchMode",
    ({ normalMode, branchMode }) => {
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
