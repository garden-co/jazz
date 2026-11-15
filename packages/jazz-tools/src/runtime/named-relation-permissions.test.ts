import { randomUUID } from "node:crypto";
import { expect, it } from "vitest";
import { deploy } from "../dev/catalogue.js";
import { startLocalJazzServer, testApp, testPermissions } from "../testing/index.js";
import { createJazzContext } from "../backend/create-jazz-context.js";
import { localFirstAccountId } from "../accounts/local-first.js";

const app = testApp({
  tables: {
    authors: {
      columns: {
        owner: "uuid",
        editable: "boolean",
        insertable: "boolean",
        removable: "boolean",
      },
    },
    bundles: {
      columns: { authorIds: { array: "uuid" }, label: "string" },
      relations: { writers: { rel: "authors", column: "authorIds" } },
    },
    collections: {
      columns: { label: "string" },
      relations: { grants: { reverse: "grants", relation: "assets" } },
    },
    grants: {
      columns: {
        resourceId: "uuid?",
        collectionIds: { array: "uuid" },
        owner: "uuid",
        editable: "boolean",
        insertable: "boolean",
        removable: "boolean",
      },
      relations: {
        asset: { rel: "resources", column: "resourceId" },
        assets: { rel: "collections", column: "collectionIds" },
      },
    },
    posts: {
      columns: { authorId: "uuid?", label: "string" },
      relations: { writer: { rel: "authors", column: "authorId" } },
    },
    resources: {
      columns: { label: "string" },
      relations: { grants: { reverse: "grants", relation: "asset" } },
    },
  },
});
const permissionsFile = {
  format: 1,
  tables: {
    authors: {
      read: "owner = session.user",
      insert: { allOf: ["owner = session.user", "insertable = true"] },
      update: { allOf: ["owner = session.user", "editable = true"] },
      delete: { allOf: ["owner = session.user", "removable = true"] },
    },
    bundles: { read: "allowedTo.read(writers)" },
    collections: {
      read: "allowedTo.read(grants)",
      insert: "allowedTo.insert(grants)",
      update: {
        old: "allowedTo.update(grants)",
        new: { allOf: ["allowedTo.update(grants)", "label != 'forbidden'"] },
      },
      delete: "allowedTo.delete(grants)",
    },
    grants: {
      read: "owner = session.user",
      insert: { allOf: ["owner = session.user", "insertable = true"] },
      update: { allOf: ["owner = session.user", "editable = true"] },
      delete: { allOf: ["owner = session.user", "removable = true"] },
    },
    posts: {
      read: "allowedTo.read(writer)",
      insert: "allowedTo.insert(writer)",
      update: "allowedTo.update(writer)",
      delete: "allowedTo.delete(writer)",
    },
    resources: {
      read: "allowedTo.read(grants)",
      insert: "allowedTo.insert(grants)",
      update: {
        old: "allowedTo.update(grants)",
        new: { allOf: ["allowedTo.update(grants)", "label != 'forbidden'"] },
      },
      delete: "allowedTo.delete(grants)",
    },
  },
};
const permissions = testPermissions(app, permissionsFile);

it.each(["forward", "reverse"])(
  "enforces named %s inheritance per identity, operation, and row image",
  async (direction) => {
    const appId = randomUUID();
    const backendSecret = `named-relations-backend-${appId}`;
    const adminSecret = `named-relations-admin-${appId}`;
    const server = await startLocalJazzServer({ appId, backendSecret, adminSecret });
    let context: ReturnType<typeof createJazzContext> | undefined;
    try {
      const { bundle } = await deploy({
        appId,
        serverUrl: server.url,
        adminSecret,
        app,
        permissions: permissionsFile,
      });
      context = createJazzContext({
        bundle,
        appId,
        app,
        permissions,
        driver: { type: "memory" },
        serverUrl: server.url,
        backendSecret,
        env: "test",
      });
      const backend = context.asBackend(app);
      const owner = (name: string) => localFirstAccountId(name, "named-relations-tests");
      const session = (name: string) =>
        context!.forSession(
          {
            issuer: "named-relations-tests",
            user_id: name,
            account_id: owner(name),
            claims: {},
            authMode: "external",
          },
          app,
        );
      const alice = session("alice");
      const bob = session("bob");
      const yes = { editable: true, insertable: true, removable: true };
      const no = { editable: false, insertable: false, removable: false };
      if (direction === "forward") {
        const a = await backend
          .insert(app.authors, { owner: owner("alice"), ...yes })
          .wait({ tier: "global" });
        const b = await backend
          .insert(app.authors, { owner: owner("bob"), ...yes })
          .wait({ tier: "global" });
        const reader = await backend
          .insert(app.authors, { owner: owner("alice"), ...no })
          .wait({ tier: "global" });
        const post = await backend
          .insert(app.posts, { authorId: a.id, label: "alice" })
          .wait({ tier: "global" });
        await backend.insert(app.posts, { authorId: b.id, label: "bob" }).wait({ tier: "global" });
        await backend.insert(app.posts, { authorId: null, label: "null" }).wait({ tier: "global" });
        await backend
          .insert(app.posts, { authorId: randomUUID(), label: "missing" })
          .wait({ tier: "global" });
        const readOnlyPost = await backend
          .insert(app.posts, { authorId: reader.id, label: "read-only" })
          .wait({ tier: "global" });
        expect((await alice.all(app.posts)).map((row) => row.label).sort()).toEqual([
          "alice",
          "read-only",
        ]);
        expect((await bob.all(app.posts)).map((row) => row.label)).toEqual(["bob"]);
        await backend
          .insert(app.bundles, { authorIds: [b.id, a.id, a.id], label: "mixed" })
          .wait({ tier: "global" });
        await backend
          .insert(app.bundles, { authorIds: [], label: "empty" })
          .wait({ tier: "global" });
        await backend
          .insert(app.bundles, { authorIds: [b.id], label: "bob-only" })
          .wait({ tier: "global" });
        expect((await alice.all(app.bundles)).map((row) => row.label)).toEqual(["mixed"]);

        await expect(
          alice.update(app.posts, post.id, { label: "updated" }).wait({ tier: "global" }),
        ).resolves.toBeUndefined();
        expect(await backend.one(app.posts.where({ id: post.id }))).toMatchObject({
          label: "updated",
        });
        await expect(
          Promise.resolve().then(() =>
            alice
              .insert(app.posts, { authorId: reader.id, label: "denied" })
              .wait({ tier: "global" }),
          ),
        ).rejects.toThrow(/AuthorizationDenied|Write rejected|read policy denied/);
        await expect(
          alice.insert(app.posts, { authorId: a.id, label: "inserted" }).wait({ tier: "global" }),
        ).resolves.toMatchObject({ label: "inserted" });
        await expect(
          Promise.resolve().then(() =>
            alice.update(app.posts, post.id, { authorId: b.id }).wait({ tier: "global" }),
          ),
        ).rejects.toThrow(/AuthorizationDenied|Write rejected|read policy denied/);
        await expect(
          Promise.resolve().then(() =>
            bob.update(app.posts, post.id, { authorId: b.id }).wait({ tier: "global" }),
          ),
        ).rejects.toThrow(/AuthorizationDenied|Write rejected|read policy denied/);
        await expect(
          Promise.resolve().then(() =>
            alice.delete(app.posts, readOnlyPost.id).wait({ tier: "global" }),
          ),
        ).rejects.toThrow(/AuthorizationDenied|Write rejected|read policy denied/);
        await expect(
          alice.delete(app.posts, post.id).wait({ tier: "global" }),
        ).resolves.toBeUndefined();
      } else {
        const resource = await backend
          .insert(app.resources, { label: "shared" })
          .wait({ tier: "global" });
        const other = await backend
          .insert(app.resources, { label: "other" })
          .wait({ tier: "global" });
        await backend.insert(app.resources, { label: "unreferenced" }).wait({ tier: "global" });
        const collection = await backend
          .insert(app.collections, { label: "array" })
          .wait({ tier: "global" });
        await backend.insert(app.collections, { label: "unreferenced" }).wait({ tier: "global" });
        // Multiple witnesses include a different identity and an operation-denied row.
        await backend
          .insert(app.grants, {
            resourceId: resource.id,
            collectionIds: [collection.id],
            owner: owner("bob"),
            ...yes,
          })
          .wait({ tier: "global" });
        await backend
          .insert(app.grants, {
            resourceId: resource.id,
            collectionIds: [collection.id],
            owner: owner("alice"),
            ...no,
          })
          .wait({ tier: "global" });
        const witness = await backend
          .insert(app.grants, {
            resourceId: resource.id,
            collectionIds: [collection.id, collection.id],
            owner: owner("alice"),
            ...yes,
          })
          .wait({ tier: "global" });
        await backend
          .insert(app.grants, {
            resourceId: other.id,
            collectionIds: [],
            owner: owner("bob"),
            ...yes,
          })
          .wait({ tier: "global" });
        await backend
          .insert(app.grants, {
            resourceId: null,
            collectionIds: [],
            owner: owner("alice"),
            ...yes,
          })
          .wait({ tier: "global" });
        expect((await alice.all(app.resources)).map((row) => row.label)).toEqual(["shared"]);
        expect((await alice.all(app.collections)).map((row) => row.label)).toEqual(["array"]);
        await expect(
          alice.update(app.resources, resource.id, { label: "allowed" }).wait({ tier: "global" }),
        ).resolves.toBeUndefined();
        expect(await backend.one(app.resources.where({ id: resource.id }))).toMatchObject({
          label: "allowed",
        });
        const insertId = randomUUID();
        await backend
          .insert(app.grants, {
            resourceId: insertId,
            collectionIds: [],
            owner: owner("alice"),
            ...yes,
          })
          .wait({ tier: "global" });
        await expect(
          alice
            .insert(app.resources, { label: "new resource" }, { id: insertId })
            .wait({ tier: "global" }),
        ).resolves.toMatchObject({ label: "new resource" });
        await expect(
          Promise.resolve().then(() =>
            alice
              .update(app.resources, resource.id, { label: "forbidden" })
              .wait({ tier: "global" }),
          ),
        ).rejects.toThrow(/AuthorizationDenied|Write rejected|read policy denied/);
        await expect(
          Promise.resolve().then(() =>
            alice
              .update(app.resources, other.id, { label: "wrong identity" })
              .wait({ tier: "global" }),
          ),
        ).rejects.toThrow(/AuthorizationDenied|Write rejected|read policy denied/);
        await backend.delete(app.grants, witness.id).wait({ tier: "global" });
        // Read access survives through the reader witness; update/delete authority does not.
        expect(
          (await alice.all(app.resources.where({ id: resource.id }))).map((row) => row.label),
        ).toEqual(["allowed"]);
        await expect(
          Promise.resolve().then(() =>
            alice.update(app.resources, resource.id, { label: "denied" }).wait({ tier: "global" }),
          ),
        ).rejects.toThrow(/AuthorizationDenied|Write rejected|read policy denied/);
        await expect(
          Promise.resolve().then(() =>
            alice.delete(app.resources, resource.id).wait({ tier: "global" }),
          ),
        ).rejects.toThrow(/AuthorizationDenied|Write rejected|read policy denied/);
        await expect(
          Promise.resolve().then(() =>
            alice.delete(app.collections, collection.id).wait({ tier: "global" }),
          ),
        ).rejects.toThrow(/AuthorizationDenied|Write rejected|read policy denied/);
        await expect(
          bob.delete(app.resources, resource.id).wait({ tier: "global" }),
        ).resolves.toBeUndefined();
        const deniedId = randomUUID();
        await backend
          .insert(app.grants, {
            resourceId: deniedId,
            collectionIds: [],
            owner: owner("alice"),
            ...no,
          })
          .wait({ tier: "global" });
        await expect(
          Promise.resolve().then(() =>
            alice
              .insert(app.resources, { label: "denied resource" }, { id: deniedId })
              .wait({ tier: "global" }),
          ),
        ).rejects.toThrow(/AuthorizationDenied|Write rejected|read policy denied/);
      }
    } finally {
      await context?.shutdown();
      await server.stop();
    }
  },
  60_000,
);

it("rejects cycles in named reverse inheritance during schema validation", async () => {
  const cyclic = testApp({
    tables: {
      lefts: {
        columns: { rightId: "uuid?" },
        relations: {
          right: { rel: "rights", column: "rightId" },
          incoming: { reverse: "rights", relation: "left" },
        },
      },
      rights: {
        columns: { leftId: "uuid?" },
        relations: {
          left: { rel: "lefts", column: "leftId" },
          incoming: { reverse: "lefts", relation: "right" },
        },
      },
    },
  });
  // The permissions loader validates the expansion, so the cycle is refused
  // before a server starts.
  await expect(
    Promise.resolve().then(() =>
      startLocalJazzServer({
        schema: cyclic,
        permissions: testPermissions(cyclic, {
          tables: {
            lefts: { read: "allowedTo.read(incoming)" },
            rights: { read: "allowedTo.read(incoming)" },
          },
        }),
      }),
    ),
  ).rejects.toThrow(/cyclic policy expansion under INHERITS_REFERENCING/);
});
