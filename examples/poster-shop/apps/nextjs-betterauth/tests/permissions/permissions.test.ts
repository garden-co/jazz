import { afterEach, beforeEach, expect, it } from "vitest";
import { createPolicyTestApp, type PolicyTestApp } from "jazz-tools/testing";
import { app } from "../../schema.js";
import permissions from "../../permissions.js";

let testApp: PolicyTestApp;
const issuer = "https://poster-shop.test";
const authorFor = (userId: string) => JSON.stringify([issuer, userId]);
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
    claims: {},
    authMode: "external",
  });
  const editor = testApp.as({
    issuer,
    user_id: editorId,
    claims: {},
    authMode: "external",
  });
  const sameSubjectOtherIssuer = testApp.as({
    issuer: "https://other-poster-provider.test",
    user_id: editorId,
    claims: {},
    authMode: "external",
  });
  const canvas = await owner
    .insert(app.canvases, { title: "Poster", width: 1080, height: 1350 })
    .wait({ tier: "edge" });
  await owner
    .insert(app.canvasMembers, {
      canvasId: canvas.id,
      memberAuthor: authorFor(ownerId),
      role: "admin",
    })
    .wait({ tier: "edge" });
  const membership = await owner
    .insert(app.canvasMembers, {
      canvasId: canvas.id,
      memberAuthor: authorFor(editorId),
      role: "editor",
    })
    .wait({ tier: "edge" });
  const layer = await owner
    .insert(app.layers, { canvasId: canvas.id, name: "Art", zIndex: 0, visible: true })
    .wait({ tier: "edge" });
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
    .wait({ tier: "edge" });
  expect(shape.layerId).toBe(layer.id);
  expect(shape.canvasId).toBe(canvas.id);
  await owner.delete(app.canvasMembers, membership.id).wait({ tier: "edge" });
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
    claims: {},
    authMode: "external",
  });
  const editor = testApp.as({
    issuer,
    user_id: editorId,
    claims: {},
    authMode: "external",
  });
  const viewer = testApp.as({
    issuer,
    user_id: viewerId,
    claims: {},
    authMode: "external",
  });
  const canvas = await owner
    .insert(app.canvases, { title: "Deterministic canvas", width: 1080, height: 1350 })
    .wait({ tier: "edge" });
  for (const [userId, role] of [
    [ownerId, "admin"],
    [editorId, "editor"],
    [viewerId, "viewer"],
  ] as const) {
    await owner
      .insert(app.canvasMembers, { canvasId: canvas.id, memberAuthor: authorFor(userId), role })
      .wait({ tier: "edge" });
  }
  const [back, front] = await Promise.all([
    editor
      .insert(app.layers, { canvasId: canvas.id, name: "Back", zIndex: 0, visible: true })
      .wait({ tier: "edge" }),
    editor
      .insert(app.layers, { canvasId: canvas.id, name: "Front", zIndex: 1, visible: true })
      .wait({ tier: "edge" }),
  ]);
  const ordered = await viewer.all(
    app.layers.where({ canvasId: canvas.id }).orderBy("zIndex", "asc"),
    {
      tier: "edge",
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
    .wait({ tier: "edge" });
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
  await editor.expectDenied((db) => db.update(app.layers, back.id, { visible: false }));
  await owner.expectDenied((db) =>
    db.insert(app.assets, {
      canvasId: canvas.id,
      name: "unowned.png",
      mimeType: "image/png",
      byteLength: 1,
    }),
  );
  await owner.expectDenied((db) => db.delete(app.checkpoints, checkpoint.id));
});

it("denies cross-canvas shapes even for an admin of both canvases", async () => {
  const ownerId = "cross-canvas-owner";
  const owner = testApp.as({
    issuer,
    user_id: ownerId,
    claims: {},
    authMode: "external",
  });
  const createCanvas = async (title: string) => {
    const canvas = await owner
      .insert(app.canvases, { title, width: 1080, height: 1350 })
      .wait({ tier: "edge" });
    await owner
      .insert(app.canvasMembers, {
        canvasId: canvas.id,
        memberAuthor: authorFor(ownerId),
        role: "admin",
      })
      .wait({ tier: "edge" });
    return canvas;
  };
  const [left, right] = await Promise.all([createCanvas("Left"), createCanvas("Right")]);
  const foreignLayer = await owner
    .insert(app.layers, { canvasId: right.id, name: "Foreign", zIndex: 0, visible: true })
    .wait({ tier: "edge" });
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

it("keeps cursor creation default-deny until its ownership semantics are specified", async () => {
  const ownerId = "cursor-owner";
  const editorId = "cursor-editor";
  const owner = testApp.as({
    issuer,
    user_id: ownerId,
    claims: {},
    authMode: "external",
  });
  const editor = testApp.as({
    issuer,
    user_id: editorId,
    claims: {},
    authMode: "external",
  });
  const canvas = await owner
    .insert(app.canvases, { title: "Presence", width: 1, height: 1 })
    .wait({ tier: "edge" });
  for (const [userId, role] of [
    [ownerId, "admin"],
    [editorId, "editor"],
  ] as const) {
    await owner
      .insert(app.canvasMembers, { canvasId: canvas.id, memberAuthor: authorFor(userId), role })
      .wait({ tier: "edge" });
  }
  await editor.expectDenied((db) =>
    db.insert(app.cursors, {
      canvasId: canvas.id,
      author: authorFor(editorId),
      x: 3,
      y: 4,
      color: "#f00",
    }),
  );
  await owner.expectDenied((db) =>
    db.insert(app.cursors, {
      canvasId: canvas.id,
      author: authorFor(editorId),
      x: 1,
      y: 1,
      color: "#000",
    }),
  );
});
