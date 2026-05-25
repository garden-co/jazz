import { randomUUID } from "node:crypto";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it, onTestFinished } from "vitest";
import { schema as s } from "../index.js";
import { createJazzContext } from "../backend/create-jazz-context.js";

const app = s.defineApp({
  projects: s.table({
    name: s.string(),
    ownerId: s.string(),
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
});

const permissions = s.definePermissions(app, ({ policy, session }) => {
  policy.projects.allowRead.where({ ownerId: session.user_id });
  policy.projects.allowInsert.where({ ownerId: session.user_id });

  policy.branches.allowRead.where({ ownerId: session.user_id });
  policy.branches.allowInsert.where({ ownerId: session.user_id });

  policy.forBranch(policy.branches, ({ $branch, branchPolicy }) => {
    branchPolicy.todos.allowRead.where({ projectId: $branch.projectId });
    branchPolicy.todos.allowInsert.where({
      projectId: $branch.projectId,
      ownerId: session.user_id,
    });
  });
});

const branchReadDenyPermissions = s.definePermissions(app, ({ policy, session }) => {
  policy.projects.allowRead.always();
  policy.projects.allowInsert.always();
  policy.projects.allowUpdate.always();
  policy.projects.allowDelete.always();

  policy.branches.allowRead.always();
  policy.branches.allowInsert.where({ ownerId: session.user_id });
  policy.branches.allowUpdate
    .whereOld({ ownerId: session.user_id })
    .whereNew({ ownerId: session.user_id });
  policy.branches.allowDelete.where({ ownerId: session.user_id });

  policy.todos.allowRead.always();
  policy.todos.allowInsert.always();
  policy.todos.allowUpdate.always();
  policy.todos.allowDelete.always();

  policy.forBranch(policy.branches, ({ branchPolicy, $branch: _branch }) => {
    branchPolicy.todos.allowRead.never();
    branchPolicy.todos.allowInsert.where({ ownerId: session.user_id });
    branchPolicy.todos.allowUpdate
      .whereOld({ ownerId: session.user_id })
      .whereNew({ ownerId: session.user_id });
    branchPolicy.todos.allowDelete.where({ ownerId: session.user_id });
  });
});

const snakeCaseApp = s.defineApp({
  branches: s.table({
    name: s.string(),
    owner_id: s.string(),
  }),
  todos: s.table({
    title: s.string(),
    owner_id: s.string(),
  }),
});

const snakeCaseBranchReadDenyPermissions = s.definePermissions(
  snakeCaseApp,
  ({ policy, session }) => {
    policy.branches.allowRead.always();
    policy.branches.allowInsert.where({ owner_id: session.user_id });
    policy.branches.allowUpdate
      .whereOld({ owner_id: session.user_id })
      .whereNew({ owner_id: session.user_id });
    policy.branches.allowDelete.where({ owner_id: session.user_id });

    policy.todos.allowRead.always();
    policy.todos.allowInsert.always();
    policy.todos.allowUpdate.always();
    policy.todos.allowDelete.always();

    policy.forBranch(policy.branches, ({ branchPolicy, $branch: _branch }) => {
      branchPolicy.todos.allowRead.never();
      branchPolicy.todos.allowInsert.where({ owner_id: session.user_id });
      branchPolicy.todos.allowUpdate
        .whereOld({ owner_id: session.user_id })
        .whereNew({ owner_id: session.user_id });
      branchPolicy.todos.allowDelete.where({ owner_id: session.user_id });
    });
  },
);

type Actor = "alice" | "bob" | "carol";
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
  wait(options: { tier: "edge" }): Promise<T>;
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

async function expectMutationAllowed<T>(
  label: string,
  mutate: () => MutationHandle<T>,
): Promise<T> {
  try {
    const mutation = mutate();
    return await mutation.wait({ tier: "edge" });
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
    await mutation.wait({ tier: "edge" });
  } catch (error) {
    expect(String(error), label).toMatch(/permission_denied|policy denied/i);
    return;
  }

  throw new Error(`${label}: expected mutation to be denied`);
}

function session(userId: string) {
  return {
    user_id: userId,
    claims: {},
    authMode: "external" as const,
  };
}

async function createTestContext(testPermissions = permissions) {
  const dataRoot = await mkdtemp(join(tmpdir(), "jazz-branch-permissions-"));
  const context = createJazzContext({
    appId: `branch-permissions-${randomUUID()}`,
    app,
    permissions: testPermissions,
    driver: { type: "persistent", dataPath: join(dataRoot, "runtime.db") },
    env: "test",
    userBranch: "main",
    tier: "edge",
  });

  onTestFinished(async () => {
    context.flush();
    await context.shutdown();
    await new Promise((resolve) => setTimeout(resolve, 50));
    await rm(dataRoot, { recursive: true, force: true });
  });

  return context;
}

async function createSnakeCaseTestContext(testPermissions = snakeCaseBranchReadDenyPermissions) {
  const dataRoot = await mkdtemp(join(tmpdir(), "jazz-branch-permissions-"));
  const context = createJazzContext({
    appId: `branch-permissions-${randomUUID()}`,
    app: snakeCaseApp,
    permissions: testPermissions,
    driver: { type: "persistent", dataPath: join(dataRoot, "runtime.db") },
    env: "test",
    userBranch: "main",
    tier: "edge",
  });

  onTestFinished(async () => {
    context.flush();
    await context.shutdown();
    await new Promise((resolve) => setTimeout(resolve, 50));
    await rm(dataRoot, { recursive: true, force: true });
  });

  return context;
}

type SnakeCaseContext = Awaited<ReturnType<typeof createSnakeCaseTestContext>>;

function snakeCaseBackendDb(context: SnakeCaseContext) {
  return context.withAttribution("test-backend", snakeCaseApp);
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

async function seedMainTodo(context: SnakeCaseContext, owner: Actor, title: string) {
  return await snakeCaseBackendDb(context)
    .insert(snakeCaseApp.todos, {
      title,
      owner_id: owner,
    })
    .wait({ tier: "edge" });
}

async function seedBranch(context: SnakeCaseContext, owner: Actor, name: string) {
  return await snakeCaseBackendDb(context)
    .insert(snakeCaseApp.branches, {
      name,
      owner_id: owner,
    })
    .wait({ tier: "edge" });
}

async function seedBranchTodo(
  context: SnakeCaseContext,
  branchId: string,
  owner: Actor,
  title: string,
) {
  return await snakeCaseBackendDb(context)
    .branch(branchId)
    .insert(snakeCaseApp.todos, {
      title,
      owner_id: owner,
    })
    .wait({ tier: "edge" });
}

describe("branch permissions integration", () => {
  describe.each(readMatrixCases)(
    "read matrix: normal=$normalMode branch=$branchMode",
    ({ normalMode, branchMode }) => {
      it("applies normal read policy on main and branch read policy on branches", async () => {
        const context = await createSnakeCaseTestContext(
          defineSnakeCaseCrudPermissions({
            actionUnderTest: "read",
            normalMode,
            branchMode,
          }),
        );
        const caseName = `read normal=${normalMode} branch=${branchMode}`;
        const mainTitle = `Main todo ${caseName}`;
        const branchTitle = `Branch todo ${caseName}`;
        const mainOwner = "alice";
        const branchTodoOwner = "bob";
        const mainTodo = await seedMainTodo(context, mainOwner, mainTitle);
        const branch = await seedBranch(context, "alice", `Draft ${caseName}`);
        const branchTodo = await seedBranchTodo(context, branch.id, branchTodoOwner, branchTitle);

        for (const reader of actors) {
          const readerDb = context.forSession(session(reader), snakeCaseApp);
          const readerCase = `${caseName} reader=${reader}`;
          const expectedMain = simplePolicyAllows(normalMode, reader, mainOwner)
            ? expectedTodo(mainTodo, mainTitle, mainOwner)
            : [];
          const expectedBranch = simplePolicyAllows(branchMode, reader, branchTodoOwner)
            ? expectedTodo(branchTodo, branchTitle, branchTodoOwner)
            : [];

          await expect(
            readerDb.all(snakeCaseApp.todos.where({ title: mainTitle })),
            `${readerCase} main title local`,
          ).resolves.toEqual(expectedMain);
          await expect(
            readerDb.all(snakeCaseApp.todos.where({ title: mainTitle }), { tier: "edge" }),
            `${readerCase} main title edge`,
          ).resolves.toEqual(expectedMain);
          await expect(
            readerDb.all(snakeCaseApp.todos.where({ title: branchTitle })),
            `${readerCase} branch title on main local`,
          ).resolves.toEqual([]);
          await expect(
            readerDb.all(snakeCaseApp.todos.where({ title: branchTitle }), { tier: "edge" }),
            `${readerCase} branch title on main edge`,
          ).resolves.toEqual([]);
          await expect(
            readerDb.all(snakeCaseApp.todos.where({})),
            `${readerCase} all main local`,
          ).resolves.toEqual(expectedMain);
          await expect(
            readerDb.all(snakeCaseApp.todos.where({}), { tier: "edge" }),
            `${readerCase} all main edge`,
          ).resolves.toEqual(expectedMain);
          await expect(
            readerDb.branch(branch.id).all(snakeCaseApp.todos.where({ title: branchTitle })),
            `${readerCase} branch db local`,
          ).resolves.toEqual(expectedBranch);
          await expect(
            readerDb.branch(branch.id).all(snakeCaseApp.todos.where({ title: branchTitle }), {
              tier: "edge",
            }),
            `${readerCase} branch db edge`,
          ).resolves.toEqual(expectedBranch);
          await expect(
            readerDb.all(snakeCaseApp.todos.where({ title: branchTitle }).branch(branch.id)),
            `${readerCase} query branch local`,
          ).resolves.toEqual(expectedBranch);
          await expect(
            readerDb.all(snakeCaseApp.todos.where({ title: branchTitle }).branch(branch.id), {
              tier: "edge",
            }),
            `${readerCase} query branch edge`,
          ).resolves.toEqual(expectedBranch);
          await expect(
            readerDb
              .branch(branch.id)
              .all(snakeCaseApp.todos.where({ title: branchTitle }).branch(branch.id)),
            `${readerCase} branch db with query branch local`,
          ).resolves.toEqual(expectedBranch);
          await expect(
            readerDb
              .branch(branch.id)
              .all(snakeCaseApp.todos.where({ title: branchTitle }).branch(branch.id), {
                tier: "edge",
              }),
            `${readerCase} branch db with query branch edge`,
          ).resolves.toEqual(expectedBranch);
        }
      });
    },
  );

  describe.each(insertMatrixCases)(
    "insert matrix: normal=$normalMode branch=$branchMode",
    ({ normalMode, branchMode }) => {
      it("applies insert policy for the targeted branch only", async () => {
        const context = await createSnakeCaseTestContext(
          defineSnakeCaseCrudPermissions({
            actionUnderTest: "insert",
            normalMode,
            branchMode,
          }),
        );
        const caseName = `insert normal=${normalMode} branch=${branchMode}`;
        const branch = await seedBranch(context, "alice", `Draft ${caseName}`);

        for (const scenario of simpleMutationScenarios) {
          const writerDb = context.forSession(session(scenario.actor), snakeCaseApp);
          const mainTitle = `Main ${caseName} ${scenario.name}`;
          const branchTitle = `Branch ${caseName} ${scenario.name}`;
          const mainAllowed = simplePolicyAllows(normalMode, scenario.actor, scenario.owner);
          const branchAllowed = simplePolicyAllows(branchMode, scenario.actor, scenario.owner);

          if (mainAllowed) {
            const row = await expectMutationAllowed(`${caseName} main ${scenario.name}`, () =>
              writerDb.insert(snakeCaseApp.todos, {
                title: mainTitle,
                owner_id: scenario.owner,
              }),
            );
            await expect(
              writerDb.all(snakeCaseApp.todos.where({ title: mainTitle }), { tier: "edge" }),
              `${caseName} main inserted row survives ${scenario.name}`,
            ).resolves.toEqual(expectedTodo(row, mainTitle, scenario.owner));
          } else {
            await expectMutationDenied(`${caseName} main ${scenario.name}`, () =>
              writerDb.insert(snakeCaseApp.todos, {
                title: mainTitle,
                owner_id: scenario.owner,
              }),
            );
            await expect(
              snakeCaseBackendDb(context).all(snakeCaseApp.todos.where({ title: mainTitle }), {
                tier: "edge",
              }),
              `${caseName} main denied row absent ${scenario.name}`,
            ).resolves.toEqual([]);
          }

          if (branchAllowed) {
            const row = await expectMutationAllowed(`${caseName} branch ${scenario.name}`, () =>
              writerDb.branch(branch.id).insert(snakeCaseApp.todos, {
                title: branchTitle,
                owner_id: scenario.owner,
              }),
            );
            await expect(
              writerDb
                .branch(branch.id)
                .all(snakeCaseApp.todos.where({ title: branchTitle }), { tier: "edge" }),
              `${caseName} branch inserted row survives ${scenario.name}`,
            ).resolves.toEqual(expectedTodo(row, branchTitle, scenario.owner));
          } else {
            await expectMutationDenied(`${caseName} branch ${scenario.name}`, () =>
              writerDb.branch(branch.id).insert(snakeCaseApp.todos, {
                title: branchTitle,
                owner_id: scenario.owner,
              }),
            );
            await expect(
              snakeCaseBackendDb(context)
                .branch(branch.id)
                .all(snakeCaseApp.todos.where({ title: branchTitle }), { tier: "edge" }),
              `${caseName} branch denied row absent ${scenario.name}`,
            ).resolves.toEqual([]);
          }
        }
      });
    },
  );

  describe.each(updateMatrixCases)(
    "update matrix: normal=$normalMode branch=$branchMode",
    ({ normalMode, branchMode }) => {
      it("applies update policy for the targeted branch only", async () => {
        const context = await createSnakeCaseTestContext(
          defineSnakeCaseCrudPermissions({
            actionUnderTest: "update",
            normalMode,
            branchMode,
          }),
        );
        const caseName = `update normal=${normalMode} branch=${branchMode}`;
        const branch = await seedBranch(context, "alice", `Draft ${caseName}`);

        for (const scenario of updateMutationScenarios) {
          const writerDb = context.forSession(session(scenario.actor), snakeCaseApp);
          const mainTitle = `Main ${caseName} ${scenario.name}`;
          const mainNewTitle = `${mainTitle} updated`;
          const branchTitle = `Branch ${caseName} ${scenario.name}`;
          const branchNewTitle = `${branchTitle} updated`;
          const mainRow = await seedMainTodo(context, scenario.oldOwner, mainTitle);
          const branchRow = await seedBranchTodo(
            context,
            branch.id,
            scenario.oldOwner,
            branchTitle,
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
              writerDb.update(snakeCaseApp.todos, mainRow.id, {
                title: mainNewTitle,
                owner_id: scenario.newOwner,
              }),
            );
            await expect(
              snakeCaseBackendDb(context).all(snakeCaseApp.todos.where({ id: mainRow.id }), {
                tier: "edge",
              }),
              `${caseName} main allowed update persisted ${scenario.name}`,
            ).resolves.toEqual(expectedTodo(mainRow, mainNewTitle, scenario.newOwner));
          } else {
            await expectMutationDenied(`${caseName} main ${scenario.name}`, () =>
              writerDb.update(snakeCaseApp.todos, mainRow.id, {
                title: mainNewTitle,
                owner_id: scenario.newOwner,
              }),
            );
            await expect(
              snakeCaseBackendDb(context).all(snakeCaseApp.todos.where({ id: mainRow.id }), {
                tier: "edge",
              }),
              `${caseName} main denied update preserved ${scenario.name}`,
            ).resolves.toEqual(expectedTodo(mainRow, mainTitle, scenario.oldOwner));
          }

          if (branchAllowed) {
            await expectMutationAllowed(`${caseName} branch ${scenario.name}`, () =>
              writerDb.branch(branch.id).update(snakeCaseApp.todos, branchRow.id, {
                title: branchNewTitle,
                owner_id: scenario.newOwner,
              }),
            );
            await expect(
              snakeCaseBackendDb(context)
                .branch(branch.id)
                .all(snakeCaseApp.todos.where({ id: branchRow.id }), { tier: "edge" }),
              `${caseName} branch allowed update persisted ${scenario.name}`,
            ).resolves.toEqual(expectedTodo(branchRow, branchNewTitle, scenario.newOwner));
          } else {
            await expectMutationDenied(`${caseName} branch ${scenario.name}`, () =>
              writerDb.branch(branch.id).update(snakeCaseApp.todos, branchRow.id, {
                title: branchNewTitle,
                owner_id: scenario.newOwner,
              }),
            );
            await expect(
              snakeCaseBackendDb(context)
                .branch(branch.id)
                .all(snakeCaseApp.todos.where({ id: branchRow.id }), { tier: "edge" }),
              `${caseName} branch denied update preserved ${scenario.name}`,
            ).resolves.toEqual(expectedTodo(branchRow, branchTitle, scenario.oldOwner));
          }
        }
      });
    },
  );

  describe.each(deleteMatrixCases)(
    "delete matrix: normal=$normalMode branch=$branchMode",
    ({ normalMode, branchMode }) => {
      it("applies delete policy for the targeted branch only", async () => {
        const context = await createSnakeCaseTestContext(
          defineSnakeCaseCrudPermissions({
            actionUnderTest: "delete",
            normalMode,
            branchMode,
          }),
        );
        const caseName = `delete normal=${normalMode} branch=${branchMode}`;
        const branch = await seedBranch(context, "alice", `Draft ${caseName}`);

        for (const scenario of simpleMutationScenarios) {
          const writerDb = context.forSession(session(scenario.actor), snakeCaseApp);
          const mainTitle = `Main ${caseName} ${scenario.name}`;
          const branchTitle = `Branch ${caseName} ${scenario.name}`;
          const mainRow = await seedMainTodo(context, scenario.owner, mainTitle);
          const branchRow = await seedBranchTodo(context, branch.id, scenario.owner, branchTitle);
          const mainAllowed = simplePolicyAllows(normalMode, scenario.actor, scenario.owner);
          const branchAllowed = simplePolicyAllows(branchMode, scenario.actor, scenario.owner);

          if (mainAllowed) {
            await expectMutationAllowed(`${caseName} main ${scenario.name}`, () =>
              writerDb.delete(snakeCaseApp.todos, mainRow.id),
            );
            await expect(
              snakeCaseBackendDb(context).all(snakeCaseApp.todos.where({ id: mainRow.id }), {
                tier: "edge",
              }),
              `${caseName} main allowed delete removed row ${scenario.name}`,
            ).resolves.toEqual([]);
          } else {
            await expectMutationDenied(`${caseName} main ${scenario.name}`, () =>
              writerDb.delete(snakeCaseApp.todos, mainRow.id),
            );
            await expect(
              snakeCaseBackendDb(context).all(snakeCaseApp.todos.where({ id: mainRow.id }), {
                tier: "edge",
              }),
              `${caseName} main denied delete preserved row ${scenario.name}`,
            ).resolves.toEqual(expectedTodo(mainRow, mainTitle, scenario.owner));
          }

          if (branchAllowed) {
            await expectMutationAllowed(`${caseName} branch ${scenario.name}`, () =>
              writerDb.branch(branch.id).delete(snakeCaseApp.todos, branchRow.id),
            );
            await expect(
              snakeCaseBackendDb(context)
                .branch(branch.id)
                .all(snakeCaseApp.todos.where({ id: branchRow.id }), { tier: "edge" }),
              `${caseName} branch allowed delete removed row ${scenario.name}`,
            ).resolves.toEqual([]);
          } else {
            await expectMutationDenied(`${caseName} branch ${scenario.name}`, () =>
              writerDb.branch(branch.id).delete(snakeCaseApp.todos, branchRow.id),
            );
            await expect(
              snakeCaseBackendDb(context)
                .branch(branch.id)
                .all(snakeCaseApp.todos.where({ id: branchRow.id }), { tier: "edge" }),
              `${caseName} branch denied delete preserved row ${scenario.name}`,
            ).resolves.toEqual(expectedTodo(branchRow, branchTitle, scenario.owner));
          }
        }
      });
    },
  );

  it("uses branch read policy for branch todos while main todos remain readable", async () => {
    const context = await createSnakeCaseTestContext();
    const aliceDb = context.forSession(session("alice"), snakeCaseApp);

    const mainTodo = await aliceDb
      .insert(snakeCaseApp.todos, {
        title: "Main todo",
        owner_id: "alice",
      })
      .wait({ tier: "local" });
    const branch = await aliceDb
      .insert(snakeCaseApp.branches, {
        name: "Alice's draft",
        owner_id: "alice",
      })
      .wait({ tier: "local" });

    const branchDb = aliceDb.branch(branch.id);
    await branchDb
      .insert(snakeCaseApp.todos, {
        title: "Branch todo",
        owner_id: "alice",
      })
      .wait({ tier: "local" });

    await expect(aliceDb.all(snakeCaseApp.todos.where({}))).resolves.toEqual([
      expect.objectContaining({
        id: mainTodo.id,
        title: "Main todo",
        owner_id: "alice",
      }),
    ]);
    await expect(branchDb.all(snakeCaseApp.todos.where({}))).resolves.toEqual([]);
  });

  it("uses branch read policy instead of normal table read policy for branch reads", async () => {
    const context = await createTestContext(branchReadDenyPermissions);
    const aliceDb = context.forSession(session("alice"), app);

    const project = await aliceDb
      .insert(app.projects, {
        name: "Branching docs",
        ownerId: "alice",
      })
      .wait({ tier: "local" });

    const branch = await aliceDb
      .insert(app.branches, {
        projectId: project.id,
        name: "Alice's draft",
        ownerId: "alice",
      })
      .wait({ tier: "local" });

    const branchDb = aliceDb.branch(branch.id);

    await branchDb
      .insert(app.todos, {
        projectId: branch.projectId,
        title: "Invisible draft todo",
        ownerId: "alice",
      })
      .wait({ tier: "edge" });

    await expect(branchDb.all(app.todos.where({}))).resolves.toEqual([]);
    await expect(branchDb.all(app.todos.where({}), { tier: "edge" })).resolves.toEqual([]);
    await expect(aliceDb.all(app.todos.branch(branch.id))).resolves.toEqual([]);
    await expect(aliceDb.all(app.todos.branch(branch.id), { tier: "edge" })).resolves.toEqual([]);
    await expect(
      aliceDb.all(
        app.projects
          .where({ id: project.id })
          .include({ todosViaProject: app.todos.branch(branch.id) }),
      ),
    ).resolves.toEqual([
      expect.objectContaining({
        todosViaProject: [],
      }),
    ]);
  });

  it("does not fall back to normal table read policy for non-row-id branch names", async () => {
    const context = await createTestContext(branchReadDenyPermissions);
    const aliceDb = context.forSession(session("alice"), app);
    const draftDb = aliceDb.branch("alice-draft");
    const backendDraftDb = context.withAttribution("alice").branch("alice-draft");

    await backendDraftDb
      .insert(app.todos, {
        projectId: randomUUID(),
        title: "Invalid branch todo",
        ownerId: "alice",
      })
      .wait({ tier: "local" });

    await expect(draftDb.all(app.todos.where({}))).resolves.toEqual([]);
  });

  it("does not fall back to normal table insert policy for non-row-id branch names", async () => {
    const context = await createTestContext(branchReadDenyPermissions);
    const aliceDb = context.forSession(session("alice"), app);
    const draftDb = aliceDb.branch("alice-draft");

    await expectMutationDenied("named branch insert uses branch policy", () =>
      draftDb.insert(app.todos, {
        projectId: randomUUID(),
        title: "Denied named branch todo",
        ownerId: "bob",
      }),
    );
  });

  it("uses an app-created branch row as the branch db backing row", async () => {
    const context = await createTestContext();
    const aliceDb = context.forSession(session("alice"), app);

    const project = await aliceDb
      .insert(app.projects, {
        name: "Branching docs",
        ownerId: "alice",
      })
      .wait({ tier: "local" });

    const branch = await aliceDb
      .insert(app.branches, {
        projectId: project.id,
        name: "Alice's draft",
        ownerId: "alice",
      })
      .wait({ tier: "local" });

    const branchDb = aliceDb.branch(branch.id);

    await branchDb
      .insert(app.todos, {
        projectId: branch.projectId,
        title: "Write API docs",
        ownerId: "alice",
      })
      .wait({ tier: "local" });

    await expect(branchDb.all(app.todos.where({}))).resolves.toEqual([
      expect.objectContaining({
        projectId: branch.projectId,
        title: "Write API docs",
        ownerId: "alice",
      }),
    ]);
  });

  it("requires normal read access to the branch backing row", async () => {
    const context = await createTestContext();
    const aliceDb = context.forSession(session("alice"), app);

    const project = await aliceDb
      .insert(app.projects, {
        name: "Branching docs",
        ownerId: "alice",
      })
      .wait({ tier: "local" });
    const branch = await aliceDb
      .insert(app.branches, {
        projectId: project.id,
        name: "Alice's draft",
        ownerId: "alice",
      })
      .wait({ tier: "local" });

    await aliceDb
      .branch(branch.id)
      .insert(app.todos, {
        projectId: project.id,
        title: "Private draft todo",
        ownerId: "alice",
      })
      .wait({ tier: "local" });

    const bobBranchDb = context.forSession(session("bob"), app).branch(branch.id);

    await expect(bobBranchDb.all(app.todos.where({}))).resolves.toEqual([]);
  });

  it("composes query-level branch ids when overriding a branch db view", async () => {
    const context = await createTestContext();
    const aliceDb = context.forSession(session("alice"), app);

    const project = await aliceDb
      .insert(app.projects, {
        name: "Branching docs",
        ownerId: "alice",
      })
      .wait({ tier: "local" });
    const branchA = await aliceDb
      .insert(app.branches, {
        projectId: project.id,
        name: "Alice's draft",
        ownerId: "alice",
      })
      .wait({ tier: "local" });
    const branchB = await aliceDb
      .insert(app.branches, {
        projectId: project.id,
        name: "Alice's alternate draft",
        ownerId: "alice",
      })
      .wait({ tier: "local" });

    await aliceDb
      .branch(branchB.id)
      .insert(app.todos, {
        projectId: project.id,
        title: "Write alternate API docs",
        ownerId: "alice",
      })
      .wait({ tier: "local" });

    const rows = await aliceDb.branch(branchA.id).all(app.todos.branch(branchB.id));

    expect(rows).toEqual([
      expect.objectContaining({
        title: "Write alternate API docs",
      }),
    ]);
  });
});
