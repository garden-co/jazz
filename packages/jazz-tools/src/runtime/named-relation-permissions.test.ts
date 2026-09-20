import { randomUUID } from "node:crypto";
import { expect, it } from "vitest";
import { schema as s } from "../index.js";
import { deploy } from "../dev/catalogue.js";
import { startLocalJazzServer } from "../testing/index.js";
import { createJazzContext } from "../backend/create-jazz-context.js";
import { localFirstAccountId } from "../accounts/local-first.js";

const app = s.defineApp({
  authors: s.table(
    { owner: s.uuid(), editable: s.boolean(), insertable: s.boolean(), removable: s.boolean() },
    {},
  ),
  posts: s.table(
    { authorId: s.uuid().optional(), label: s.string() },
    { writer: s.rel("authors", "authorId") },
  ),
  bundles: s.table(
    { authorIds: s.array(s.uuid()), label: s.string() },
    { writers: s.rel("authors", "authorIds") },
  ),
  resources: s.table({ label: s.string() }, { grants: s.reverse("grants", "asset") }),
  collections: s.table({ label: s.string() }, { grants: s.reverse("grants", "assets") }),
  grants: s.table(
    {
      resourceId: s.uuid().optional(),
      collectionIds: s.array(s.uuid()),
      owner: s.uuid(),
      editable: s.boolean(),
      insertable: s.boolean(),
      removable: s.boolean(),
    },
    {
      asset: s.rel("resources", "resourceId"),
      assets: s.rel("collections", "collectionIds"),
    },
  ),
});
const permissions = s.definePermissions(app, ({ policy, allowedTo, session, allOf }) => {
  for (const table of [policy.authors, policy.grants]) {
    table.allowRead.where({ owner: session.user.account });
    table.allowInsert.where({ owner: session.user.account, insertable: true });
    table.allowUpdate
      .whereOld({ owner: session.user.account, editable: true })
      .whereNew({ owner: session.user.account, editable: true });
    table.allowDelete.where({ owner: session.user.account, removable: true });
  }
  policy.posts.allowRead.where(allowedTo.read("writer"));
  policy.posts.allowInsert.where(allowedTo.insert("writer"));
  policy.posts.allowUpdate
    .whereOld(allowedTo.update("writer"))
    .whereNew(allowedTo.update("writer"));
  policy.posts.allowDelete.where(allowedTo.delete("writer"));
  policy.bundles.allowRead.where(allowedTo.read("writers"));
  for (const table of [policy.resources, policy.collections]) {
    table.allowRead.where(allowedTo.read("grants"));
    table.allowInsert.where(allowedTo.insert("grants"));
    table.allowUpdate
      .whereOld(allowedTo.update("grants"))
      .whereNew(allOf([allowedTo.update("grants"), { label: { ne: "forbidden" } }]));
    table.allowDelete.where(allowedTo.delete("grants"));
  }
});

it.each(["forward", "reverse"])(
  "enforces named %s inheritance per identity, operation, and row image",
  async (direction) => {
    const appId = randomUUID();
    const backendSecret = `named-relations-backend-${appId}`;
    const adminSecret = `named-relations-admin-${appId}`;
    const server = await startLocalJazzServer({ appId, backendSecret, adminSecret });
    let context: ReturnType<typeof createJazzContext> | undefined;
    try {
      await deploy({ appId, serverUrl: server.url, adminSecret, schema: app, permissions });
      context = createJazzContext({
        appId,
        app,
        permissions,
        driver: { type: "memory" },
        serverUrl: server.url,
        backendSecret,
        env: "test",
        tier: "edge",
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
          .wait({ tier: "edge" });
        const b = await backend
          .insert(app.authors, { owner: owner("bob"), ...yes })
          .wait({ tier: "edge" });
        const reader = await backend
          .insert(app.authors, { owner: owner("alice"), ...no })
          .wait({ tier: "edge" });
        const post = await backend
          .insert(app.posts, { authorId: a.id, label: "alice" })
          .wait({ tier: "edge" });
        await backend.insert(app.posts, { authorId: b.id, label: "bob" }).wait({ tier: "edge" });
        await backend.insert(app.posts, { authorId: null, label: "null" }).wait({ tier: "edge" });
        await backend
          .insert(app.posts, { authorId: randomUUID(), label: "missing" })
          .wait({ tier: "edge" });
        const readOnlyPost = await backend
          .insert(app.posts, { authorId: reader.id, label: "read-only" })
          .wait({ tier: "edge" });
        expect((await alice.all(app.posts)).map((row) => row.label).sort()).toEqual([
          "alice",
          "read-only",
        ]);
        expect((await bob.all(app.posts)).map((row) => row.label)).toEqual(["bob"]);
        await backend
          .insert(app.bundles, { authorIds: [b.id, a.id, a.id], label: "mixed" })
          .wait({ tier: "edge" });
        await backend.insert(app.bundles, { authorIds: [], label: "empty" }).wait({ tier: "edge" });
        await backend
          .insert(app.bundles, { authorIds: [b.id], label: "bob-only" })
          .wait({ tier: "edge" });
        expect((await alice.all(app.bundles)).map((row) => row.label)).toEqual(["mixed"]);

        await expect(
          alice.update(app.posts, post.id, { label: "updated" }).wait({ tier: "edge" }),
        ).resolves.toBeUndefined();
        expect(await backend.one(app.posts.where({ id: post.id }))).toMatchObject({
          label: "updated",
        });
        await expect(
          alice.insert(app.posts, { authorId: reader.id, label: "denied" }).wait({ tier: "edge" }),
        ).rejects.toThrow(/AuthorizationDenied|Write rejected/);
        await expect(
          alice.insert(app.posts, { authorId: a.id, label: "inserted" }).wait({ tier: "edge" }),
        ).resolves.toMatchObject({ label: "inserted" });
        await expect(
          alice.update(app.posts, post.id, { authorId: b.id }).wait({ tier: "edge" }),
        ).rejects.toThrow(/AuthorizationDenied|Write rejected/);
        await expect(
          bob.update(app.posts, post.id, { authorId: b.id }).wait({ tier: "edge" }),
        ).rejects.toThrow(/AuthorizationDenied|Write rejected/);
        await expect(
          alice.delete(app.posts, readOnlyPost.id).wait({ tier: "edge" }),
        ).rejects.toThrow(/AuthorizationDenied|Write rejected/);
        await expect(
          alice.delete(app.posts, post.id).wait({ tier: "edge" }),
        ).resolves.toBeUndefined();
      } else {
        const resource = await backend
          .insert(app.resources, { label: "shared" })
          .wait({ tier: "edge" });
        const other = await backend
          .insert(app.resources, { label: "other" })
          .wait({ tier: "edge" });
        await backend.insert(app.resources, { label: "unreferenced" }).wait({ tier: "edge" });
        const collection = await backend
          .insert(app.collections, { label: "array" })
          .wait({ tier: "edge" });
        await backend.insert(app.collections, { label: "unreferenced" }).wait({ tier: "edge" });
        // Multiple witnesses include a different identity and an operation-denied row.
        await backend
          .insert(app.grants, {
            resourceId: resource.id,
            collectionIds: [collection.id],
            owner: owner("bob"),
            ...yes,
          })
          .wait({ tier: "edge" });
        await backend
          .insert(app.grants, {
            resourceId: resource.id,
            collectionIds: [collection.id],
            owner: owner("alice"),
            ...no,
          })
          .wait({ tier: "edge" });
        const witness = await backend
          .insert(app.grants, {
            resourceId: resource.id,
            collectionIds: [collection.id, collection.id],
            owner: owner("alice"),
            ...yes,
          })
          .wait({ tier: "edge" });
        await backend
          .insert(app.grants, {
            resourceId: other.id,
            collectionIds: [],
            owner: owner("bob"),
            ...yes,
          })
          .wait({ tier: "edge" });
        await backend
          .insert(app.grants, {
            resourceId: null,
            collectionIds: [],
            owner: owner("alice"),
            ...yes,
          })
          .wait({ tier: "edge" });
        expect((await alice.all(app.resources)).map((row) => row.label)).toEqual(["shared"]);
        expect((await alice.all(app.collections)).map((row) => row.label)).toEqual(["array"]);
        await expect(
          alice.update(app.resources, resource.id, { label: "allowed" }).wait({ tier: "edge" }),
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
          .wait({ tier: "edge" });
        await expect(
          alice
            .insert(app.resources, { label: "new resource" }, { id: insertId })
            .wait({ tier: "edge" }),
        ).resolves.toMatchObject({ label: "new resource" });
        await expect(
          alice.update(app.resources, resource.id, { label: "forbidden" }).wait({ tier: "edge" }),
        ).rejects.toThrow(/AuthorizationDenied|Write rejected/);
        await expect(
          alice.update(app.resources, other.id, { label: "wrong identity" }).wait({ tier: "edge" }),
        ).rejects.toThrow(/AuthorizationDenied|Write rejected/);
        await backend.delete(app.grants, witness.id).wait({ tier: "edge" });
        // Read access survives through the reader witness; update/delete authority does not.
        expect(
          (await alice.all(app.resources.where({ id: resource.id }))).map((row) => row.label),
        ).toEqual(["allowed"]);
        await expect(
          alice.update(app.resources, resource.id, { label: "denied" }).wait({ tier: "edge" }),
        ).rejects.toThrow(/AuthorizationDenied|Write rejected/);
        await expect(
          alice.delete(app.resources, resource.id).wait({ tier: "edge" }),
        ).rejects.toThrow(/AuthorizationDenied|Write rejected/);
        await expect(
          alice.delete(app.collections, collection.id).wait({ tier: "edge" }),
        ).rejects.toThrow(/AuthorizationDenied|Write rejected/);
        await expect(
          bob.delete(app.resources, resource.id).wait({ tier: "edge" }),
        ).resolves.toBeUndefined();
        const deniedId = randomUUID();
        await backend
          .insert(app.grants, {
            resourceId: deniedId,
            collectionIds: [],
            owner: owner("alice"),
            ...no,
          })
          .wait({ tier: "edge" });
        await expect(
          alice
            .insert(app.resources, { label: "denied resource" }, { id: deniedId })
            .wait({ tier: "edge" }),
        ).rejects.toThrow(/AuthorizationDenied|Write rejected/);
      }
    } finally {
      await context?.shutdown();
      await server.stop();
    }
  },
  60_000,
);

it("rejects cycles in named reverse inheritance during schema validation", async () => {
  const cyclic = s.defineApp({
    lefts: s.table(
      { rightId: s.uuid().optional() },
      { right: s.rel("rights", "rightId"), incoming: s.reverse("rights", "left") },
    ),
    rights: s.table(
      { leftId: s.uuid().optional() },
      { left: s.rel("lefts", "leftId"), incoming: s.reverse("lefts", "right") },
    ),
  });
  const cyclicPermissions = s.definePermissions(cyclic, ({ policy, allowedTo }) => {
    policy.lefts.allowRead.where(allowedTo.read("incoming"));
    policy.rights.allowRead.where(allowedTo.read("incoming"));
  });
  await expect(
    startLocalJazzServer({ schema: cyclic, permissions: cyclicPermissions }),
  ).rejects.toThrow(/cyclic policy expansion under INHERITS_REFERENCING/);
});
