import { afterEach, beforeEach, expect, it } from "vitest";
import { createPolicyTestApp, type PolicyTestApp } from "jazz-tools/testing";
import { app } from "../../schema.js";
import permissions from "../../permissions.js";

let testApp: PolicyTestApp;
const issuer = "https://poster-shop.test";
const accountIds = new Map<string, string>();

function authorFor(userId: string, identityIssuer = issuer): string {
  const key = [identityIssuer, userId].join("\u0000");
  let accountId = accountIds.get(key);
  if (!accountId) {
    accountId = crypto.randomUUID();
    accountIds.set(key, accountId);
  }
  return accountId;
}
beforeEach(async () => {
  testApp = await createPolicyTestApp(app, permissions, expect);
});
afterEach(async () => testApp.shutdown());

it("allows an admin to bootstrap a canvas and an editor to add same-canvas shapes", async () => {
  const ownerId = "poster-owner",
    editorId = "poster-editor";
  const owner = testApp.as({
    issuer,
    user_id: ownerId,
    account_id: authorFor(ownerId),
    claims: {},
    authMode: "external",
  });
  const editor = testApp.as({
    issuer,
    user_id: editorId,
    account_id: authorFor(editorId),
    claims: {},
    authMode: "external",
  });
  const sameSubjectOtherIssuer = testApp.as({
    issuer: "https://other-poster-provider.test",
    user_id: editorId,
    account_id: authorFor(editorId, "https://other-poster-provider.test"),
    claims: {},
    authMode: "external",
  });
  const canvas = await owner
    .insert(app.canvases, { title: "Poster", width: 1080, height: 1350 })
    .wait({ tier: "global" });
  await owner
    .insert(app.canvasMembers, {
      canvasId: canvas.id,
      memberAuthor: authorFor(ownerId),
      role: "admin",
    })
    .wait({ tier: "global" });
  const membership = await owner
    .insert(app.canvasMembers, {
      canvasId: canvas.id,
      memberAuthor: authorFor(editorId),
      role: "editor",
    })
    .wait({ tier: "global" });
  const layer = await owner
    .insert(app.layers, { canvasId: canvas.id, name: "Art", zIndex: 0, visible: true })
    .wait({ tier: "global" });
  await sameSubjectOtherIssuer.expectDenied((db) =>
    db.insert(app.shapes, {
      canvasId: canvas.id,
      layerId: layer.id,
      kind: "rect",
      x: -1,
      y: -1,
      width: 1,
      height: 1,
      rotation: 0,
      zIndex: -1,
      fill: "#f00",
    }),
  );
  const shape = await editor
    .insert(app.shapes, {
      canvasId: canvas.id,
      layerId: layer.id,
      kind: "rect",
      x: 0,
      y: 0,
      width: 1,
      height: 1,
      rotation: 0,
      zIndex: 0,
      fill: "#fff",
    })
    .wait({ tier: "global" });
  expect(shape.layerId).toBe(layer.id);
  expect(shape.canvasId).toBe(canvas.id);
  await owner.delete(app.canvasMembers, membership.id).wait({ tier: "global" });
  await editor.expectDenied((db) =>
    db.insert(app.shapes, {
      canvasId: canvas.id,
      layerId: layer.id,
      kind: "rect",
      x: 1,
      y: 1,
      width: 1,
      height: 1,
      rotation: 0,
      zIndex: 1,
      fill: "#000",
    }),
  );
});

it("keeps canvas ordering and history markers behind the same membership boundary", async () => {
  const ownerId = "canvas-owner";
  const editorId = "canvas-editor";
  const viewerId = "canvas-viewer";
  const owner = testApp.as({
    issuer,
    user_id: ownerId,
    account_id: authorFor(ownerId),
    claims: {},
    authMode: "external",
  });
  const editor = testApp.as({
    issuer,
    user_id: editorId,
    account_id: authorFor(editorId),
    claims: {},
    authMode: "external",
  });
  const viewer = testApp.as({
    issuer,
    user_id: viewerId,
    account_id: authorFor(viewerId),
    claims: {},
    authMode: "external",
  });
  const canvas = await owner
    .insert(app.canvases, { title: "Deterministic canvas", width: 1080, height: 1350 })
    .wait({ tier: "global" });
  for (const [userId, role] of [
    [ownerId, "admin"],
    [editorId, "editor"],
    [viewerId, "viewer"],
  ] as const) {
    await owner
      .insert(app.canvasMembers, { canvasId: canvas.id, memberAuthor: authorFor(userId), role })
      .wait({ tier: "global" });
  }
  const [back, front] = await Promise.all([
    editor
      .insert(app.layers, { canvasId: canvas.id, name: "Back", zIndex: 0, visible: true })
      .wait({ tier: "global" }),
    editor
      .insert(app.layers, { canvasId: canvas.id, name: "Front", zIndex: 1, visible: true })
      .wait({ tier: "global" }),
  ]);
  const ordered = await viewer.all(
    app.layers.where({ canvasId: canvas.id }).orderBy("zIndex", "asc"),
    {
      tier: "global",
    },
  );
  expect(ordered.map((layer) => [layer.id, layer.zIndex])).toEqual([
    [back.id, 0],
    [front.id, 1],
  ]);
  await editor.expectDenied((db) =>
    db.insert(app.checkpoints, { canvasId: canvas.id, label: "forged", branch: "main" }),
  );
  const checkpoint = await owner
    .insert(app.checkpoints, { canvasId: canvas.id, label: "Approved poster", branch: "main" })
    .wait({ tier: "global" });
  await editor.expectDenied((db) =>
    db.update(app.checkpoints, checkpoint.id, { label: "rewritten" }),
  );
  await viewer.expectDenied((db) =>
    db.insert(app.shapes, {
      canvasId: canvas.id,
      layerId: back.id,
      kind: "rect",
      x: 0,
      y: 0,
      width: 1,
      height: 1,
      rotation: 0,
      zIndex: 0,
      fill: "#000",
    }),
  );
  await viewer.expectDenied((db) => db.update(app.layers, back.id, { visible: false }));
  await viewer.expectDenied((db) =>
    db.insert(app.assets, {
      canvasId: canvas.id,
      name: "viewer.png",
      mimeType: "image/png",
      byteLength: 1,
      width: 1,
      height: 1,
      bytes: new Uint8Array([1]),
    }),
  );
  await owner.expectDenied((db) => db.delete(app.checkpoints, checkpoint.id));
});

it("denies cross-canvas shapes even for an admin of both canvases", async () => {
  const ownerId = "cross-canvas-owner";
  const owner = testApp.as({
    issuer,
    user_id: ownerId,
    account_id: authorFor(ownerId),
    claims: {},
    authMode: "external",
  });
  const createCanvas = async (title: string) => {
    const canvas = await owner
      .insert(app.canvases, { title, width: 1080, height: 1350 })
      .wait({ tier: "global" });
    await owner
      .insert(app.canvasMembers, {
        canvasId: canvas.id,
        memberAuthor: authorFor(ownerId),
        role: "admin",
      })
      .wait({ tier: "global" });
    return canvas;
  };
  const [left, right] = await Promise.all([createCanvas("Left"), createCanvas("Right")]);
  const foreignLayer = await owner
    .insert(app.layers, { canvasId: right.id, name: "Foreign", zIndex: 0, visible: true })
    .wait({ tier: "global" });
  await owner.expectDenied((db) =>
    db.insert(app.shapes, {
      canvasId: left.id,
      layerId: foreignLayer.id,
      kind: "rect",
      x: 0,
      y: 0,
      width: 1,
      height: 1,
      rotation: 0,
      zIndex: 0,
      fill: "#000",
    }),
  );
});

function actor(userId: string) {
  return testApp.as({
    issuer,
    user_id: userId,
    account_id: authorFor(userId),
    claims: {},
    authMode: "external",
  });
}

async function canvasWithRoles(
  title: string,
  ownerId: string,
  others: readonly (readonly [string, "viewer" | "editor" | "admin"])[],
) {
  const owner = actor(ownerId);
  const canvas = await owner
    .insert(app.canvases, { title, width: 1080, height: 1350 })
    .wait({ tier: "global" });
  for (const [userId, role] of [[ownerId, "admin"] as const, ...others]) {
    await owner
      .insert(app.canvasMembers, { canvasId: canvas.id, memberAuthor: authorFor(userId), role })
      .wait({ tier: "global" });
  }
  const layer = await owner
    .insert(app.layers, { canvasId: canvas.id, name: "Art", zIndex: 0, visible: true })
    .wait({ tier: "global" });
  return { owner, canvas, layer };
}

function rect(canvasId: string, layerId: string, zIndex = 0) {
  return {
    canvasId,
    layerId,
    kind: "rect" as const,
    x: 0,
    y: 0,
    width: 10,
    height: 10,
    rotation: 0,
    zIndex,
    fill: "ink",
  };
}

it("lets every member publish only their own cursor (#1926)", async () => {
  const { owner, canvas } = await canvasWithRoles("Presence", "cursor-owner", [
    ["cursor-editor", "editor"],
    ["cursor-viewer", "viewer"],
  ]);
  const editor = actor("cursor-editor");
  const viewer = actor("cursor-viewer");
  const outsider = actor("cursor-outsider");
  const cursor = (userId: string, x = 1) => ({
    canvasId: canvas.id,
    author: authorFor(userId),
    name: userId,
    x,
    y: 2,
    color: "0",
  });

  const editorCursor = await editor.insert(app.cursors, cursor("cursor-editor")).wait({
    tier: "global",
  });
  // Viewers are present on the canvas too, even though they cannot edit it.
  await viewer.insert(app.cursors, cursor("cursor-viewer")).wait({ tier: "global" });
  await editor
    .update(app.cursors, editorCursor.id, { x: 40, y: 50, active: false })
    .wait({ tier: "global" });

  // Nobody writes, reassigns or removes someone else's presence.
  await owner.expectDenied((db) => db.insert(app.cursors, cursor("cursor-editor")));
  await owner.expectDenied((db) => db.update(app.cursors, editorCursor.id, { x: 0 }));
  await owner.expectDenied((db) => db.delete(app.cursors, editorCursor.id));
  await editor.expectDenied((db) =>
    db.update(app.cursors, editorCursor.id, { author: authorFor("cursor-owner") }),
  );
  // Non-members cannot appear on the canvas at all.
  await outsider.expectDenied((db) => db.insert(app.cursors, cursor("cursor-outsider")));

  await editor.delete(app.cursors, editorCursor.id).wait({ tier: "global" });
});

it("lets editors manage layers and shapes but honours layer locks", async () => {
  const { owner, canvas, layer } = await canvasWithRoles("Layers", "layer-owner", [
    ["layer-editor", "editor"],
    ["layer-viewer", "viewer"],
  ]);
  const editor = actor("layer-editor");
  const viewer = actor("layer-viewer");
  const shape = await editor.insert(app.shapes, rect(canvas.id, layer.id)).wait({
    tier: "global",
  });
  await editor
    .update(app.shapes, shape.id, { x: 20, fill: "red", rotation: 15 })
    .wait({ tier: "global" });
  await editor.update(app.layers, layer.id, { name: "Artwork", zIndex: 3 }).wait({
    tier: "global",
  });
  await viewer.expectDenied((db) => db.update(app.shapes, shape.id, { x: 99 }));
  await viewer.expectDenied((db) => db.delete(app.shapes, shape.id));

  await editor.update(app.layers, layer.id, { locked: true }).wait({ tier: "global" });
  // A locked layer freezes its shapes for everyone, admins included.
  await owner.expectDenied((db) => db.update(app.shapes, shape.id, { x: 1 }));
  await editor.expectDenied((db) => db.delete(app.shapes, shape.id));
  await editor.expectDenied((db) => db.insert(app.shapes, rect(canvas.id, layer.id, 1)));
  await editor.expectDenied((db) => db.delete(app.layers, layer.id));

  await editor.update(app.layers, layer.id, { locked: false }).wait({ tier: "global" });
  await editor.delete(app.shapes, shape.id).wait({ tier: "global" });
  await editor.delete(app.layers, layer.id).wait({ tier: "global" });
});

it("denies moving layers or shapes onto a canvas the user cannot edit", async () => {
  const mine = await canvasWithRoles("Mine", "mover", []);
  const theirs = await canvasWithRoles("Theirs", "other-owner", [["mover", "viewer"]]);
  const mover = actor("mover");
  const shape = await mover.insert(app.shapes, rect(mine.canvas.id, mine.layer.id)).wait({
    tier: "global",
  });
  await mover.expectDenied((db) =>
    db.update(app.layers, mine.layer.id, { canvasId: theirs.canvas.id }),
  );
  await mover.expectDenied((db) =>
    db.update(app.shapes, shape.id, { layerId: theirs.layer.id, canvasId: theirs.canvas.id }),
  );
  // Same canvas id, foreign layer: still denied by the layer correlation.
  await mover.expectDenied((db) => db.update(app.shapes, shape.id, { layerId: theirs.layer.id }));
});

it("lets editors upload immutable image assets as large values", async () => {
  const { canvas } = await canvasWithRoles("Assets", "asset-owner", [
    ["asset-editor", "editor"],
    ["asset-viewer", "viewer"],
  ]);
  const editor = actor("asset-editor");
  const viewer = actor("asset-viewer");
  const bytes = new Uint8Array(1024).map((_, index) => index % 251);
  const asset = await editor
    .insert(app.assets, {
      canvasId: canvas.id,
      name: "texture.png",
      mimeType: "image/png",
      byteLength: bytes.byteLength,
      width: 32,
      height: 32,
      bytes,
    })
    .wait({ tier: "global" });
  const [page] = await viewer.all(
    app.assets.where({ id: asset.id }).select({ bytes: { from: 100, to: 110 } }),
    { tier: "global" },
  );
  expect(Array.from(page!.bytes)).toEqual(Array.from(bytes.slice(100, 110)));
  await editor.expectDenied((db) => db.update(app.assets, asset.id, { name: "renamed.png" }));
  await viewer.expectDenied((db) => db.delete(app.assets, asset.id));
  await editor.delete(app.assets, asset.id).wait({ tier: "global" });
});

it("keeps invites visible to and issued by admins only", async () => {
  const { canvas } = await canvasWithRoles("Invites", "invite-owner", [
    ["invite-editor", "editor"],
  ]);
  const owner = actor("invite-owner");
  const editor = actor("invite-editor");
  const invite = await owner
    .insert(app.canvasInvites, { canvasId: canvas.id, token: crypto.randomUUID(), role: "editor" })
    .wait({ tier: "global" });
  await editor.expectDenied((db) =>
    db.insert(app.canvasInvites, {
      canvasId: canvas.id,
      token: crypto.randomUUID(),
      role: "editor",
    }),
  );
  expect(
    await editor.all(app.canvasInvites.where({ canvasId: canvas.id }), { tier: "global" }),
  ).toEqual([]);
  await editor.expectDenied((db) => db.delete(app.canvasInvites, invite.id));
  // A client can never self-admit with a token; redeeming is server-side.
  await actor("invite-stranger").expectDenied((db) =>
    db.insert(app.canvasMembers, {
      canvasId: canvas.id,
      memberAuthor: authorFor("invite-stranger"),
      role: "editor",
    }),
  );
  await owner.delete(app.canvasInvites, invite.id).wait({ tier: "global" });
});
