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
  const owner = testApp.as({ user_id: ownerId, claims: {}, authMode: "external" });
  const editor = testApp.as({ user_id: editorId, claims: {}, authMode: "external" });
  const canvas = (
    await owner
      .insert(app.canvases, { title: "Poster", width: 1080, height: 1350 })
      .wait({ tier: "edge" })
  ).value;
  await owner
    .insert(app.canvasMembers, { canvasId: canvas.id, userId: ownerId, role: "admin" })
    .wait({ tier: "edge" });
  const membership = (
    await owner
      .insert(app.canvasMembers, { canvasId: canvas.id, userId: editorId, role: "editor" })
      .wait({ tier: "edge" })
  ).value;
  const layer = (
    await owner
      .insert(app.layers, { canvasId: canvas.id, name: "Art", zIndex: 0, visible: true })
      .wait({ tier: "edge" })
  ).value;
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
