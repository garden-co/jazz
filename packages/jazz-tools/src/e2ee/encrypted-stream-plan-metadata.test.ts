import { expect, it, vi } from "vitest";
import { JazzClient } from "../runtime/client.js";
import { createJazzContext } from "../backend/create-jazz-context.js";
import { NativeRuntimeAdapter } from "../runtime/native-runtime/native-runtime-adapter.js";
import { schema as s } from "../schema-namespace.js";
import { definePermissions } from "../permissions/index.js";
import { createDb } from "../runtime/default-create-db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { deviceRequestSchema, groupSchema, spaceSchema } from "./managed-schema.js";
import { frontE2eeCore } from "./fixtures/app.js";

const app = s.defineApp(
  {
    ...deviceRequestSchema,
    ...groupSchema,
    ...spaceSchema,
    projects: s.table({ title: s.string() }, {}),
    files: s
      .table(
        {
          projectId: s.uuid(),
          payload: s.bytes(),
          name: s.string(),
          publicBytes: s.bytes(),
          notes: s.string(),
        },
        { project: s.rel("projects", "projectId") },
      )
      .encrypted({ space: "projectId", columns: ["payload", "name"] }),
    prices: s.table(
      {
        amount: s.float().transform({
          dependsOn: ["scale"],
          from: (value, context: Readonly<{ scale: number }>) => value / context.scale,
          to: (value: number, context: Readonly<{ scale: number }>) => value * context.scale,
        }),
        scale: s.float().default(10),
        body: s.bytes(),
      },
      {},
    ),
    labels: s.table(
      {
        label: s.string().transform({
          dependsOn: ["id"],
          from: (value, context: Readonly<{ id: string }>) => {
            if (!value.startsWith(`${context.id}:`))
              throw new Error("Wrong transform row identity");
            return value.slice(context.id.length + 1);
          },
          to: (value: string, context: Readonly<{ id: string }>) => `${context.id}:${value}`,
        }),
      },
      {},
    ),
  },
  { migrations: "none" },
);
const physical = s.defineApp(
  {
    ...deviceRequestSchema,
    ...groupSchema,
    ...spaceSchema,
    projects: s.table({ title: s.string() }, {}),
    files: s.table(
      {
        projectId: s.uuid(),
        payload: s.bytes(),
        name: s.bytes(),
        publicBytes: s.bytes(),
        notes: s.string(),
      },
      { project: s.rel("projects", "projectId") },
    ),
    prices: s.table({ amount: s.float(), scale: s.float().default(10), body: s.bytes() }, {}),
    labels: s.table({ label: s.string() }, {}),
  },
  { migrations: "none" },
);

async function fixture() {
  let server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
  const coreUrl = server.url;
  const permissions = definePermissions(app, ({ policy, session }) => {
    policy.projects.allowRead.always();
    policy.projects.allowInsert.where({ "$createdBy.account": session.user.account });
    policy.files.allowRead.always();
    policy.files.allowInsert.where({ "$createdBy.account": session.user.account });
    policy.files.allowUpdate.where({ "$createdBy.account": session.user.account });
    policy.prices.allowRead.where({ "$createdBy.account": session.user.account });
    policy.prices.allowInsert.where({ "$createdBy.account": session.user.account });
    policy.prices.allowUpdate.where({ "$createdBy.account": session.user.account });
    policy.labels.allowRead.where({ "$createdBy.account": session.user.account });
    policy.labels.allowInsert.where({ "$createdBy.account": session.user.account });
    policy.__e2ee_spaces.allowRead.always();
    policy.__e2ee_spaces.allowInsert.where({ creatorAccountId: session.user.account });
    policy.__e2ee_space_grants.allowRead.always();
    policy.__e2ee_space_grants.allowInsert.where({ authorAccountId: session.user.account });
    policy.__e2ee_space_deliveries.allowRead.always();
    policy.__e2ee_space_deliveries.allowInsert.where({ senderAccountId: session.user.account });
    policy.__e2ee_space_successors.allowRead.always();
    policy.__e2ee_space_successors.allowInsert.where({ authorAccountId: session.user.account });
  });
  const clients: Awaited<ReturnType<typeof createDb>>[] = [];
  try {
    const deployment = {
      serverUrl: server.url,
      appId: server.appId,
      adminSecret: server.adminSecret,
      schema: physical,
      permissions,
    };
    const { bundle } = await deploy(deployment);
    server = await frontE2eeCore(server, deployment);
    const account = await localAccountConfig(server.appId, server.url);
    let saved: string | null = null;
    const owner = await createDb({
      ...account,
      e2ee: {
        app,
        store: {
          async read() {
            return saved;
          },
          async update(transform: (current: string | null) => string) {
            saved = transform(saved);
          },
        },
      },
    });
    clients.push(owner);
    const legacy = await createDb(account);
    clients.push(legacy);
    await owner.e2ee.devices.list();
    const scope = owner.insert(app.projects, { title: "Existing" });
    await scope.wait({ tier: "global" });
    return {
      owner,
      legacy,
      scope: scope.value,
      server,
      permissions,
      bundle,
      coreUrl,
      clients,
      async close() {
        await Promise.all(clients.map((db) => db.shutdown()));
        await server.stop();
      },
    };
  } catch (error) {
    await Promise.all(clients.map((db) => db.shutdown()));
    await server.stop();
    throw error;
  }
}
function fileData(scope: string) {
  return {
    projectId: scope,
    payload: new Uint8Array([9]),
    name: "Encrypted",
    publicBytes: new Uint8Array([5]),
    notes: "Before",
  };
}

it.each(["publicBytes", "notes"] as const)(
  "keeps a plan's plaintext stream ordinary inside an encrypted table (%s)",
  async (column) => {
    const f = await fixture();
    try {
      const file = f.owner.insert(app.files, fileData(f.scope.id));
      await file.wait({ tier: "global" });
      const before = await f.legacy.one(physical.files.where({ id: file.value.id }), {
        tier: "remote",
      });
      let consumed = 0;
      const expected = column === "publicBytes" ? new Uint8Array([1, 2, 3]) : "After";
      const source = (async function* () {
        consumed++;
        yield expected;
      })();
      const write = await f.owner.streamingTransaction((plan) =>
        column === "publicBytes"
          ? plan.updateStreaming(app.files, file.value.id, { publicBytes: source })
          : plan.updateStreaming(app.files, file.value.id, { notes: source }),
      );
      await write.wait({ tier: "global" });
      const logical = await f.owner.one(app.files.where({ id: file.value.id }), { tier: "remote" });
      const raw = await f.legacy.one(physical.files.where({ id: file.value.id }), {
        tier: "remote",
      });
      expect(consumed).toBe(1);
      expect(logical?.[column]).toEqual(expected);
      expect(raw?.[column]).toEqual(expected);
      expect(logical?.payload).toEqual(new Uint8Array([9]));
      expect(raw?.payload).toEqual(before?.payload);
      expect(raw?.name).toEqual(before?.name);
    } finally {
      await f.close();
    }
  },
  30_000,
);

it("prepares a plan metadata insert with its defaulted transform dependency", async () => {
  const f = await fixture();
  try {
    let consumed = 0;
    const source = (async function* () {
      consumed++;
      yield new Uint8Array([1]);
    })();
    const write = await f.owner.streamingTransaction((plan) => ({
      price: plan.insert(app.prices, { amount: 3, body: new Uint8Array([4]) }),
      file: plan.insertStreaming(app.files, { ...fileData(f.scope.id), payload: source }),
    }));
    const ids = await write.wait({ tier: "global" });
    expect(consumed).toBe(1);
    expect(await f.owner.one(app.prices.where({ id: ids.price.id }), { tier: "remote" })).toEqual({
      id: ids.price.id,
      amount: 3,
      scale: 10,
      body: new Uint8Array([4]),
    });
    expect(
      await f.legacy.one(physical.prices.where({ id: ids.price.id }), { tier: "remote" }),
    ).toEqual({
      id: ids.price.id,
      amount: 30,
      scale: 10,
      body: new Uint8Array([4]),
    });
    expect(
      (await f.owner.one(app.files.where({ id: ids.file.id }), { tier: "remote" }))?.payload,
    ).toEqual(new Uint8Array([1]));
  } finally {
    await f.close();
  }
}, 30_000);

it.each(["generated", "explicit"] as const)(
  "prepares a plan metadata transform against its actual %s row ID",
  async (kind) => {
    const f = await fixture();
    try {
      let consumed = 0;
      const source = (async function* () {
        consumed++;
        yield new Uint8Array([1]);
      })();
      const explicit = crypto.randomUUID();
      const write = await f.owner.streamingTransaction((plan) => ({
        label: plan.insert(
          app.labels,
          { label: "Value" },
          kind === "explicit" ? { id: explicit } : undefined,
        ),
        file: plan.insertStreaming(app.files, { ...fileData(f.scope.id), payload: source }),
      }));
      const ids = await write.wait({ tier: "global" });
      if (kind === "explicit") expect(ids.label.id).toBe(explicit);
      expect(consumed).toBe(1);
      expect(await f.owner.one(app.labels.where({ id: ids.label.id }), { tier: "remote" })).toEqual(
        { id: ids.label.id, label: "Value" },
      );
      expect(
        await f.legacy.one(physical.labels.where({ id: ids.label.id }), { tier: "remote" }),
      ).toEqual({ id: ids.label.id, label: `${ids.label.id}:Value` });
    } finally {
      await f.close();
    }
  },
  30_000,
);

it.each(["update", "upsert"] as const)(
  "prepares stored metadata dependencies for a streamed %s in its publication transaction",
  async (operation) => {
    const f = await fixture();
    try {
      const row = f.owner.insert(app.prices, { amount: 2, scale: 10, body: new Uint8Array([0]) });
      await row.wait({ tier: "global" });
      let consumed = 0;
      const source = (async function* () {
        consumed++;
        yield new Uint8Array([1]);
      })();
      const write = await f.owner.streamingTransaction((plan) =>
        operation === "update"
          ? plan.updateStreaming(app.prices, row.value.id, { amount: 3, body: source })
          : plan.upsertStreaming(app.prices, row.value.id, { amount: 3, body: source }),
      );
      await write.wait({ tier: "global" });
      expect(consumed).toBe(1);
      expect(await f.owner.one(app.prices.where({ id: row.value.id }), { tier: "remote" })).toEqual(
        { id: row.value.id, amount: 3, scale: 10, body: new Uint8Array([1]) },
      );
      expect(
        await f.legacy.one(physical.prices.where({ id: row.value.id }), { tier: "remote" }),
      ).toEqual({ id: row.value.id, amount: 30, scale: 10, body: new Uint8Array([1]) });
    } finally {
      await f.close();
    }
  },
  30_000,
);

it.each(["earlier update", "earlier insert"] as const)(
  "refuses implicit stored dependencies after a same-row %s before pulling sources",
  async (kind) => {
    const f = await fixture();
    try {
      const row = f.owner.insert(app.prices, { amount: 2, scale: 10, body: new Uint8Array([0]) });
      await row.wait({ tier: "global" });
      const id = kind === "earlier insert" ? crypto.randomUUID() : row.value.id;
      let consumed = 0;
      const source = () =>
        (async function* () {
          consumed++;
          yield new Uint8Array([1]);
        })();
      await expect(
        f.owner.streamingTransaction((plan) => {
          if (kind === "earlier insert")
            plan.insert(app.prices, { amount: 2, scale: 20, body: new Uint8Array([0]) }, { id });
          else plan.updateStreaming(app.prices, id, { scale: 20, body: source() });
          return plan.upsertStreaming(app.prices, id, { amount: 4, body: source() });
        }),
      ).rejects.toThrow(/earlier declaration.*supply all dependency columns/);
      expect(consumed).toBe(0);
      const raw = await f.legacy.one(physical.prices.where({ id }), { tier: "remote" });
      if (kind === "earlier insert") expect(raw).toBeNull();
      else expect(raw).toEqual({ id, amount: 20, scale: 10, body: new Uint8Array([0]) });
    } finally {
      await f.close();
    }
  },
  30_000,
);

it("preserves same-row declaration order when later transform dependencies are explicit", async () => {
  const f = await fixture();
  try {
    const row = f.owner.insert(app.prices, { amount: 2, scale: 10, body: new Uint8Array([0]) });
    await row.wait({ tier: "global" });
    let consumed = 0;
    const source = (n: number) =>
      (async function* () {
        consumed++;
        yield new Uint8Array([n]);
      })();
    const write = await f.owner.streamingTransaction((plan) => {
      plan.updateStreaming(app.prices, row.value.id, { scale: 20, body: source(1) });
      return plan.updateStreaming(app.prices, row.value.id, {
        amount: 4,
        scale: 20,
        body: source(2),
      });
    });
    await write.wait({ tier: "global" });
    expect(consumed).toBe(2);
    expect(
      await f.legacy.one(physical.prices.where({ id: row.value.id }), { tier: "remote" }),
    ).toEqual({ id: row.value.id, amount: 80, scale: 20, body: new Uint8Array([2]) });
  } finally {
    await f.close();
  }
}, 30_000);

it.each(["update", "upsert"] as const)(
  "does not treat a policy-hidden dependency row as absent (%s)",
  async (operation) => {
    const f = await fixture();
    const backend = createJazzContext({
      app: physical,
      permissions: f.permissions,
      bundle: f.bundle,
      appId: f.server.appId,
      serverUrl: f.coreUrl,
      backendSecret: f.server.backendSecret,
      driver: { type: "memory" },
      allowLocalFirstAuth: true,
    });
    try {
      const row = f.owner.insert(app.prices, { amount: 2, scale: 10, body: new Uint8Array([0]) });
      await row.wait({ tier: "global" });
      const bobConfig = await localAccountConfig(f.server.appId, f.server.url);
      const bob = await createDb(bobConfig);
      f.clients.push(bob);
      const token = await bob.refreshAccountAuth(bobConfig.account);
      const scoped = await backend.forRequest({
        headers: { authorization: `Bearer ${token}` },
      });
      expect(
        await scoped.one(app.prices.where({ id: row.value.id }), { tier: "remote" }),
      ).toBeNull();
      let consumed = 0;
      const source = (async function* () {
        consumed++;
        yield new Uint8Array([1]);
      })();
      await expect(
        scoped.streamingTransaction((plan) =>
          operation === "update"
            ? plan.updateStreaming(app.prices, row.value.id, { amount: 4, body: source })
            : plan.upsertStreaming(app.prices, row.value.id, { amount: 4, body: source }),
        ),
      ).rejects.toThrow(/dependency row is unavailable|scoped read cannot establish/);
      expect(consumed).toBe(0);
      expect(
        await f.legacy.one(physical.prices.where({ id: row.value.id }), { tier: "remote" }),
      ).toEqual({ id: row.value.id, amount: 20, scale: 10, body: new Uint8Array([0]) });
    } finally {
      await backend.shutdown();
      await f.close();
    }
  },
  30_000,
);

function latch() {
  let release!: () => void;
  const promise = new Promise<void>((resolve) => {
    release = resolve;
  });
  return { promise, release };
}

it("refuses changed dependency values after source staging without replaying transforms or publishing", async () => {
  const f = await fixture();
  const entered = latch(),
    resume = latch();
  try {
    const row = f.owner.insert(app.prices, { amount: 2, scale: 10, body: new Uint8Array([0]) });
    await row.wait({ tier: "global" });
    await f.legacy.one(physical.prices.where({ id: row.value.id }), { tier: "remote" });
    let consumed = 0;
    const source = (async function* () {
      consumed++;
      entered.release();
      await resume.promise;
      yield new Uint8Array([1]);
    })();
    const outcome = f.owner
      .streamingTransaction((plan) =>
        plan.updateStreaming(app.prices, row.value.id, { amount: 4, body: source }),
      )
      .then(
        async (write) => {
          await write.wait({ tier: "global" });
          return null;
        },
        (error: unknown) => error,
      )
      .catch((error: unknown) => error);
    await entered.promise;
    await f.legacy.update(physical.prices, row.value.id, { scale: 20 }).wait({ tier: "global" });
    resume.release();
    expect(await outcome).toBeInstanceOf(Error);
    expect(String(await outcome)).toMatch(
      /dependencies changed|SerializationConflict|serialization conflict/i,
    );
    expect(consumed).toBe(1);
    expect(
      await f.legacy.one(physical.prices.where({ id: row.value.id }), { tier: "remote" }),
    ).toEqual({ id: row.value.id, amount: 20, scale: 20, body: new Uint8Array([0]) });
  } finally {
    resume.release();
    await f.close();
  }
}, 30_000);

it.each(["concurrent write", "rollback"] as const)(
  "keeps the same-batch dependency recheck effective across an awaited %s",
  async (action) => {
    const f = await fixture();
    const entered = latch(),
      resume = latch();
    const clients = new Map<Parameters<JazzClient["prepareTransaction"]>[0], JazzClient>();
    let captured: Parameters<JazzClient["prepareTransaction"]>[0] | undefined;
    const prepare = JazzClient.prototype.prepareTransaction;
    const query = NativeRuntimeAdapter.prototype.query;
    const prepareSpy = vi
      .spyOn(JazzClient.prototype, "prepareTransaction")
      .mockImplementation(function (this: JazzClient, id, callback) {
        clients.set(id, this);
        return prepare.call(this, id, callback);
      });
    const querySpy = vi
      .spyOn(NativeRuntimeAdapter.prototype, "query")
      .mockImplementation(async function (this: NativeRuntimeAdapter, ...args) {
        const rows = await query.apply(this, args);
        const id = args[3]
          ? (JSON.parse(args[3]) as { transaction_id?: string }).transaction_id
          : undefined;
        if (!captured && id && args[0].includes('"prices"') && args[0].includes('"scale"')) {
          captured = id as typeof captured;
          entered.release();
          await resume.promise;
        }
        return rows;
      });
    try {
      const row = f.owner.insert(app.prices, { amount: 2, scale: 10, body: new Uint8Array([0]) });
      await row.wait({ tier: "global" });
      await f.legacy.one(physical.prices.where({ id: row.value.id }), { tier: "remote" });
      const source = (async function* () {
        yield new Uint8Array([1]);
      })();
      const outcome = f.owner
        .streamingTransaction((plan) =>
          plan.updateStreaming(app.prices, row.value.id, { amount: 4, body: source }),
        )
        .then(
          async (write) => {
            await write.wait({ tier: "global" });
            return null;
          },
          (error: unknown) => error,
        )
        .catch((error: unknown) => error);
      await entered.promise;
      expect(captured).toBeDefined();
      if (action === "rollback") await clients.get(captured!)!.rollbackTransaction(captured!);
      else
        await f.legacy
          .update(physical.prices, row.value.id, { scale: 20 })
          .wait({ tier: "global" });
      resume.release();
      const error = await outcome;
      expect(error).toBeInstanceOf(Error);
      if (action === "rollback")
        expect(String(error)).toMatch(/no longer active|rolled back|closed/i);
      else expect(error).toMatchObject({ code: "serialization_conflict" });
      expect(
        await f.legacy.one(physical.prices.where({ id: row.value.id }), { tier: "remote" }),
      ).toEqual({
        id: row.value.id,
        amount: 20,
        scale: action === "rollback" ? 10 : 20,
        body: new Uint8Array([0]),
      });
    } finally {
      resume.release();
      querySpy.mockRestore();
      prepareSpy.mockRestore();
      await f.close();
    }
  },
  30_000,
);
