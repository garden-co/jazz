import { afterEach, beforeEach, expect, it } from "vitest";
import { createPolicyTestApp, type PolicyTestApp } from "jazz-tools/testing";
import { app } from "../../schema.js";
import permissions from "../../permissions.js";

let testApp: PolicyTestApp;
beforeEach(async () => {
  testApp = await createPolicyTestApp(app, permissions, expect);
});
afterEach(async () => testApp.shutdown());

it("allows an admin to bootstrap a canvas, invite an editor, then revokes edits", async () => {
  const ownerId = "poster-owner",
    editorId = "poster-editor";
  const owner = testApp.as({
    issuer: "https://poster-shop.test",
    user_id: ownerId,
    claims: {},
    authMode: "external",
  });
  const editor = testApp.as({
    issuer: "https://poster-shop.test",
    user_id: editorId,
    claims: {},
    authMode: "external",
  });
  const canvas = await owner
    .insert(app.canvases, { title: "Poster", width: 1080, height: 1350 })
    .wait({ tier: "edge" });
  await owner
    .insert(app.canvasMembers, { canvasId: canvas.id, userId: ownerId, role: "admin" })
    .wait({ tier: "edge" });
  const membership = await owner
    .insert(app.canvasMembers, { canvasId: canvas.id, userId: editorId, role: "editor" })
    .wait({ tier: "edge" });
  const layer = await owner
    .insert(app.layers, { canvasId: canvas.id, name: "Art", zIndex: 0, visible: true })
    .wait({ tier: "edge" });
  await editor
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
    issuer: "https://poster-shop.test",
    user_id: ownerId,
    claims: {},
    authMode: "external",
  });
  const editor = testApp.as({
    issuer: "https://poster-shop.test",
    user_id: editorId,
    claims: {},
    authMode: "external",
  });
  const viewer = testApp.as({
    issuer: "https://poster-shop.test",
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
      .insert(app.canvasMembers, { canvasId: canvas.id, userId, role })
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
});

it("permits replaceable cursor presence only to its author", async () => {
  const ownerId = "cursor-owner";
  const editorId = "cursor-editor";
  const owner = testApp.as({
    issuer: "https://poster-shop.test",
    user_id: ownerId,
    claims: {},
    authMode: "external",
  });
  const editor = testApp.as({
    issuer: "https://poster-shop.test",
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
      .insert(app.canvasMembers, { canvasId: canvas.id, userId, role })
      .wait({ tier: "edge" });
  }
  const cursor = await editor
    .insert(app.cursors, { canvasId: canvas.id, userId: editorId, x: 3, y: 4, color: "#f00" })
    .wait({ tier: "edge" });
  await editor.update(app.cursors, cursor.id, { x: 5, y: 6 }).wait({ tier: "edge" });
  await owner.expectDenied((db) => db.update(app.cursors, cursor.id, { x: 99 }));
  await owner.expectDenied((db) =>
    db.insert(app.cursors, { canvasId: canvas.id, userId: editorId, x: 1, y: 1, color: "#000" }),
  );
});
