import { expect } from "vitest";
import { schema as s } from "../../schema-namespace.js";
import { definePermissions } from "../../permissions/index.js";
import { createDb } from "../../runtime/default-create-db.js";
import { localAccountConfig } from "../../runtime/testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../../testing/index.js";
import { createBrowserCrypto } from "../browser.js";
import { deviceRequestSchema, deviceRequestPermissions } from "../device-requests.js";
import { spaceSchema } from "../spaces.js";

export type EqualityValueScenario =
  | "equality-negative-zero"
  | "equality-array-zero"
  | "equality-enum-zero"
  | "equality-bigint"
  | "equality-null"
  | "equality-json"
  | "equality-nonfinite"
  | "equality-omitted-optional"
  | "equality-composite-literals";

export async function createEqualityValuesFixture() {
  const app = s.defineApp({
    ...deviceRequestSchema,
    ...spaceSchema,
    projects: s.table(
      { title: s.string() },
      {
        linkedNotesViaProject: s.reverse("linkedNotes", "projectRelation"),
        notesViaProject: s.reverse("notes", "project"),
      },
    ),
    counters: s
      .table(
        { projectId: s.uuid(), value: s.bigint() },
        { project: s.rel("projects", "projectId") },
      )
      .encrypted({ space: "projectId", columns: ["value"], indexes: { value: "equality" } }),
    events: s.table({ message: s.string() }, {}),
    publicDocuments: s.table(
      { projectId: s.uuid(), label: s.string(), body: s.json() },
      { project: s.rel("projects", "projectId") },
    ),
    privateDocuments: s
      .table(
        { projectId: s.uuid(), label: s.string(), body: s.json() },
        { project: s.rel("projects", "projectId") },
      )
      .encrypted({ space: "projectId", columns: ["body"], indexes: { body: "equality" } }),
    optionalNotes: s
      .table(
        { projectId: s.uuid(), title: s.string().optional() },
        { project: s.rel("projects", "projectId") },
      )
      .encrypted({ space: "projectId", columns: ["title"], indexes: { title: "equality" } }),
    optionalMeasurements: s
      .table(
        { projectId: s.uuid(), value: s.float().optional() },
        { project: s.rel("projects", "projectId") },
      )
      .encrypted({ space: "projectId", columns: ["value"], indexes: { value: "equality" } }),
    observations: s
      .table(
        {
          projectId: s.uuid(),
          numbers: s.array(s.bigint()),
          tagged: s.enum({ count: { value: s.bigint() } }),
          time: s.timestamp(),
        },
        { project: s.rel("projects", "projectId") },
      )
      .encrypted({
        space: "projectId",
        columns: ["numbers", "tagged", "time"],
        indexes: { numbers: "equality", tagged: "equality", time: "equality" },
      }),
    readings: s
      .table(
        {
          projectId: s.uuid(),
          reading: s.enum({ measured: { value: s.float() } }),
        },
        { project: s.rel("projects", "projectId") },
      )
      .encrypted({ space: "projectId", columns: ["reading"], indexes: { reading: "equality" } }),
    samples: s
      .table(
        { projectId: s.uuid(), values: s.array(s.float()) },
        { project: s.rel("projects", "projectId") },
      )
      .encrypted({ space: "projectId", columns: ["values"], indexes: { values: "equality" } }),
    measurements: s
      .table({ projectId: s.uuid(), value: s.float() }, { project: s.rel("projects", "projectId") })
      .encrypted({ space: "projectId", columns: ["value"], indexes: { value: "equality" } }),
    linkedNotes: s
      .table(
        { project: s.uuid(), title: s.string() },
        { projectRelation: s.rel("projects", "project") },
      )
      .encrypted({ space: "project", columns: ["title"] }),
    notes: s
      .table(
        {
          projectId: s.uuid(),
          title: s.string(),
          done: s.boolean(),
          payload: s.bytes(),
        },
        { project: s.rel("projects", "projectId") },
      )
      .encrypted({
        space: "projectId",
        columns: ["title", "payload"],
        indexes: { title: "equality" as const },
      }),
  });
  const policies = definePermissions(app, ({ policy, session }) => {
    policy.projects.allowRead.always();
    policy.events.allowRead.always();
    policy.events.allowInsert.always();
    policy.publicDocuments.allowRead.always();
    policy.publicDocuments.allowInsert.always();
    policy.privateDocuments.allowRead.always();
    policy.privateDocuments.allowInsert.always();
    policy.optionalNotes.allowRead.always();
    policy.optionalNotes.allowInsert.always();
    policy.optionalMeasurements.allowRead.always();
    policy.optionalMeasurements.allowInsert.always();
    policy.measurements.allowRead.always();
    policy.observations.allowRead.always();
    policy.observations.allowInsert.always();
    policy.counters.allowRead.always();
    policy.counters.allowInsert.always();
    policy.measurements.allowInsert.always();
    policy.samples.allowRead.always();
    policy.samples.allowInsert.always();
    policy.readings.allowRead.always();
    policy.readings.allowInsert.always();
    policy.readings.allowUpdate.always();
    policy.projects.allowInsert.always();
    policy.notes.allowRead.always();
    policy.notes.allowInsert.always();
    policy.notes.allowUpdate.always();
    policy.notes.allowDelete.always();
    policy.linkedNotes.allowRead.always();
    policy.linkedNotes.allowInsert.always();
    policy.__e2ee_spaces.allowRead.always();
    policy.__e2ee_spaces.allowInsert.where({ accountId: session.user.account });
    policy.__e2ee_space_successors.allowRead.always();
    policy.__e2ee_space_successors.allowInsert.where({ authorAccountId: session.user.account });
    policy.__e2ee_space_grants.allowRead.always();
    policy.__e2ee_space_grants.allowInsert.where({ authorAccountId: session.user.account });
    policy.__e2ee_space_deliveries.allowRead.always();
    policy.__e2ee_space_deliveries.allowInsert.where({ senderAccountId: session.user.account });
  });
  const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
  let db: Awaited<ReturnType<typeof createDb>> | undefined;
  try {
    await deploy({
      serverUrl: server.url,
      appId: server.appId,
      adminSecret: server.adminSecret,
      schema: app,
      permissions: { ...deviceRequestPermissions, ...policies },
    });
    const account = await localAccountConfig(server.appId, server.url);
    const crypto = await createBrowserCrypto();
    let saved: string | null = null;
    db = await createDb({
      ...account,
      e2ee: {
        app,
        store: {
          async read() {
            return saved;
          },
          async update(transform) {
            saved = transform(saved);
          },
        },
        crypto,
      },
    });
    const active = db;
    const initialize = async (title: string) => {
      const db = active;
      const tx = db.beginExclusiveTransaction();
      const project = tx.insert(app.projects, { title });
      const note = tx.insert(app.notes, {
        projectId: project.id,
        title: "Private title",
        done: false,
        payload: new Uint8Array([1, 2, 255]),
      });
      expect(project).not.toBeInstanceOf(Promise);
      expect(note).not.toBeInstanceOf(Promise);
      const committed = tx.commit();
      expect(committed).not.toBeInstanceOf(Promise);
      await committed.wait({ tier: "global" });
      return project;
    };
    // Value canonicalisation shares one accepted space. The two optional-string
    // cases retain separate spaces so their null rows cannot contaminate each other.
    // Cold atomic space creation remains covered by the initialization family.
    const valuesRoot = await initialize("Equality values");
    const nullRoot = await initialize("Null values");
    const omittedRoot = await initialize("Omitted values");
    return {
      app,
      db,
      valuesRoot,
      nullRoot,
      omittedRoot,
      async close() {
        await db?.shutdown();
        await server.stop();
      },
    };
  } catch (error) {
    await db?.shutdown();
    await server.stop();
    throw error;
  }
}

export async function runEqualityValueScenario(
  timing: EqualityValueScenario,
  fixture: Awaited<ReturnType<typeof createEqualityValuesFixture>>,
) {
  const { app, db } = fixture;
  const project =
    timing === "equality-null"
      ? fixture.nullRoot
      : timing === "equality-omitted-optional"
        ? fixture.omittedRoot
        : fixture.valuesRoot;
  if (timing === "equality-json") {
    const documents = [
      { label: "ordered", body: { x: 1, y: 2 } },
      { label: "reordered", body: { y: 2, x: 1 } },
      { label: "spaced", body: '{ "x": 1, "y": 2 }' },
    ];
    for (const document of documents) {
      await db
        .insert(app.publicDocuments, { projectId: project.id, ...document })
        .wait({ tier: "global" });
      await db
        .insert(app.privateDocuments, { projectId: project.id, ...document })
        .wait({ tier: "global" });
    }
    for (const { body, label } of documents) {
      const ordinary = await db.all(
        app.publicDocuments.where({ projectId: project.id, body: { eq: body } }),
        { tier: "global" },
      );
      expect(ordinary.map((row) => row.label)).toEqual([label]);
      const encrypted = await db.all(
        app.privateDocuments.where({ projectId: project.id, body: { eq: body } }),
        { tier: "global" },
      );
      expect(encrypted.map((row) => row.label)).toEqual([label]);
    }
    const query = app.privateDocuments.where({
      projectId: project.id,
      body: { eq: '{ "x": 1, "y": 2 }' },
    });
    const reader = db.beginTransaction();
    try {
      expect((await reader.all(query, { tier: "local" })).map((row) => row.label)).toEqual([
        "spaced",
      ]);
    } finally {
      await reader.rollback();
    }
    const snapshots: string[][] = [];
    let failure: Error | undefined;
    const stop = db.subscribe(
      query,
      {
        onUpdate: (rows) => snapshots.push(rows.map((row) => row.label).sort()),
        onError: (error) => {
          failure = error;
        },
      },
      { tier: "global" },
    );
    try {
      await expect.poll(() => failure ?? snapshots.at(-1), { timeout: 10_000 }).toEqual(["spaced"]);
      await db
        .insert(app.privateDocuments, {
          projectId: project.id,
          label: "second",
          body: '{ "x": 1, "y": 2 }',
        })
        .wait({ tier: "global" });
      await expect
        .poll(() => failure ?? snapshots.at(-1), { timeout: 10_000 })
        .toEqual(["second", "spaced"]);
    } finally {
      stop();
    }
    return;
  }
  if (timing === "equality-nonfinite") {
    const reader = db;
    await db
      .insert(app.optionalMeasurements, { projectId: project.id, value: null })
      .wait({ tier: "global" });
    for (const value of [NaN, Infinity, -Infinity]) {
      await expect(async () =>
        reader.all(app.optionalMeasurements.where({ projectId: project.id, value }), {
          tier: "global",
        }),
      ).rejects.toThrow("Non-finite numbers are not supported in encrypted query predicates");
    }
    return;
  }
  if (timing === "equality-omitted-optional") {
    const omitted = db.insert(app.optionalNotes, { projectId: project.id });
    const explicit = db.insert(app.optionalNotes, { projectId: project.id, title: null });
    const empty = db.insert(app.optionalNotes, { projectId: project.id, title: "" });
    await Promise.all([omitted, explicit, empty].map((row) => row.wait({ tier: "global" })));
    const rows = await db.all(
      app.optionalNotes.where({ projectId: project.id, title: { eq: null } }),
      { tier: "global" },
    );
    expect(rows.map((row) => row.id).sort()).toEqual([omitted.value.id, explicit.value.id].sort());
    expect(rows.every((row) => row.title === null)).toBe(true);
    return;
  }
  if (timing === "equality-null") {
    const absent = db.insert(app.optionalNotes, { projectId: project.id, title: null });
    const empty = db.insert(app.optionalNotes, { projectId: project.id, title: "" });
    const text = db.insert(app.optionalNotes, { projectId: project.id, title: "Present" });
    await Promise.all([absent, empty, text].map((row) => row.wait({ tier: "global" })));
    for (const row of [absent, empty, text]) {
      expect(
        await db.all(
          app.optionalNotes.where({ projectId: project.id, title: { eq: row.value.title } }),
          { tier: "global" },
        ),
      ).toEqual([row.value]);
    }
    return;
  }
  if (timing === "equality-composite-literals") {
    const instant = new Date("2026-01-02T03:04:05.006Z");
    const row = db.insert(app.observations, {
      projectId: project.id,
      numbers: [9007199254740993n],
      tagged: { type: "count", value: 9007199254740993n },
      time: instant,
    });
    await row.wait({ tier: "global" });
    expect(
      await db.all(
        app.observations.where({ projectId: project.id, numbers: [9007199254740993n] }),
        { tier: "global" },
      ),
    ).toEqual([row.value]);
    expect(
      await db.all(
        app.observations.where({
          projectId: project.id,
          tagged: { match: { type: "count", where: { value: 9007199254740993n } } },
        }),
        { tier: "global" },
      ),
    ).toEqual([row.value]);
    expect(
      await db.all(app.observations.where({ projectId: project.id, time: instant }), {
        tier: "global",
      }),
    ).toEqual([row.value]);
    expect(
      await db.all(
        app.observations.where({
          projectId: project.id,
          time: new Date(instant.getTime() + 1),
        }),
        { tier: "global" },
      ),
    ).toEqual([]);
    return;
  }
  if (timing === "equality-bigint") {
    const counter = db.insert(app.counters, {
      projectId: project.id,
      value: 9007199254740993n,
    });
    await counter.wait({ tier: "global" });
    expect(
      await db.all(app.counters.where({ projectId: project.id, value: 9007199254740993n }), {
        tier: "global",
      }),
    ).toEqual([counter.value]);
    expect(
      await db.all(app.counters.where({ projectId: project.id, value: 9007199254740992n }), {
        tier: "global",
      }),
    ).toEqual([]);
    return;
  }
  if (timing === "equality-enum-zero") {
    const reading = db.insert(app.readings, {
      projectId: project.id,
      reading: { type: "measured", value: -0 },
    });
    await reading.wait({ tier: "global" });
    expect(
      await db.all(
        app.readings.where({
          projectId: project.id,
          reading: { match: { type: "measured", where: { value: 0 } } },
        }),
        { tier: "global" },
      ),
    ).toEqual([reading.value]);
    expect(
      await db.all(
        app.readings.where({
          projectId: project.id,
          reading: { match: { type: "measured", where: { value: 1 } } },
        }),
        { tier: "global" },
      ),
    ).toEqual([]);
    await expect(
      db.all(
        app.readings.where({
          projectId: project.id,
          reading: { match: { type: "measured" } },
        }),
        { tier: "global" },
      ),
    ).rejects.toThrow("Unsupported encrypted query");
    let snapshot: unknown;
    let failure: Error | undefined;
    const stop = db.subscribe(
      app.readings.where({
        projectId: project.id,
        reading: { match: { type: "measured", where: { value: 0 } } },
      }),
      {
        onUpdate: (rows) => {
          snapshot = rows;
        },
        onError: (error) => {
          failure = error;
        },
      },
      { tier: "global" },
    );
    try {
      await expect.poll(() => failure ?? snapshot, { timeout: 10_000 }).toEqual([reading.value]);
      await db
        .update(app.readings, reading.value.id, {
          reading: { type: "measured", value: 2 },
        })
        .wait({ tier: "global" });
      await expect.poll(() => failure ?? snapshot, { timeout: 10_000 }).toEqual([]);
    } finally {
      stop();
    }
    let partialError: Error | undefined;
    const stopPartial = db.subscribe(
      app.readings.where({
        projectId: project.id,
        reading: { match: { type: "measured" } },
      }),
      {
        onUpdate: () => {},
        onError: (error) => {
          partialError = error;
        },
      },
      { tier: "global" },
    );
    try {
      await expect
        .poll(() => partialError?.message, { timeout: 10_000 })
        .toContain("Unsupported encrypted query");
    } finally {
      stopPartial();
    }
    return;
  }
  if (timing === "equality-array-zero") {
    const sample = db.insert(app.samples, { projectId: project.id, values: [-0, 2] });
    await sample.wait({ tier: "global" });
    for (const values of [
      [0, 2],
      [-0, 2],
    ]) {
      expect(
        await db.all(app.samples.where({ projectId: project.id, values }), { tier: "global" }),
      ).toEqual([sample.value]);
    }
    expect(
      await db.all(app.samples.where({ projectId: project.id, values: [0, 3] }), {
        tier: "global",
      }),
    ).toEqual([]);
    return;
  }
  if (timing === "equality-negative-zero") {
    const measurement = db.insert(app.measurements, { projectId: project.id, value: -0 });
    await measurement.wait({ tier: "global" });
    for (const value of [0, -0]) {
      expect(
        await db.all(app.measurements.where({ projectId: project.id, value }), {
          tier: "global",
        }),
      ).toEqual([measurement.value]);
    }
    return;
  }
  throw new Error(`Unhandled equality value scenario: ${timing}`);
}
