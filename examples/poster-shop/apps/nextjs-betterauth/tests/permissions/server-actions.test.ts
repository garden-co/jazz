import type { Db } from "jazz-tools";
import { createJazzSession, type JazzClient } from "jazz-tools/backend";
import { deploy, startLocalJazzServer, type LocalJazzServerHandle } from "jazz-tools/testing";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import permissions from "../../permissions.js";
import { app } from "../../schema.js";
import { ensurePersonalCanvas, seedPersonalCanvas } from "../../src/lib/bootstrap.js";
import { redeemInvite } from "../../src/lib/join.js";

// The server actions run against a real local Jazz server with backend
// authority, exactly as the API routes run them.
let server: LocalJazzServerHandle;
let session: Awaited<ReturnType<typeof createJazzSession>>;
let db: Db;

beforeAll(async () => {
  const backendSecret = "poster-shop-test-backend-secret";
  const adminSecret = "poster-shop-test-admin-secret";
  server = await startLocalJazzServer({ backendSecret, adminSecret });
  await deploy({
    appId: server.appId,
    serverUrl: server.url,
    adminSecret,
    schema: app,
    permissions,
  });
  session = await createJazzSession({
    app,
    permissions,
    appId: server.appId,
    driver: { type: "memory" },
    serverUrl: server.url,
    initial: { backendSecret },
    env: "test",
    tier: "global",
  });
  const snapshot = session.getSnapshot();
  if (snapshot.status !== "ready" || !snapshot.client)
    throw snapshot.error ?? new Error("not ready");
  db = (snapshot.client as JazzClient).db;
}, 120_000);

afterAll(async () => {
  await session?.close();
  await server?.stop();
});

const membershipsOf = (memberAuthor: string) =>
  db.all(app.canvasMembers.where({ memberAuthor }), { tier: "global" });

describe("ensurePersonalCanvas", () => {
  it("seeds one demo poster and is idempotent", async () => {
    const account = crypto.randomUUID();
    const first = await ensurePersonalCanvas(db, account, "Ada");
    const again = await ensurePersonalCanvas(db, account, "Ada");
    expect(again).toBe(first);
    const memberships = await membershipsOf(account);
    expect(memberships.map((row) => [row.canvasId, row.role])).toEqual([[first, "admin"]]);
    const canvas = await db.one(app.canvases.where({ id: first }), { tier: "global" });
    expect(canvas?.title).toBe("Ada's poster");
    const layers = await db.all(app.layers.where({ canvasId: first }), { tier: "global" });
    expect(layers.map((layer) => layer.name).sort()).toEqual(["Artwork", "Background", "Type"]);
    const checkpoints = await db.all(app.checkpoints.where({ canvasId: first }), {
      tier: "global",
    });
    expect(checkpoints).toHaveLength(1);
  });

  it("gives two concurrent first opens exactly one canvas", async () => {
    // In one process the calls share a promise.
    const account = crypto.randomUUID();
    const [left, right] = await Promise.all([
      ensurePersonalCanvas(db, account, "Grace"),
      ensurePersonalCanvas(db, account, "Grace"),
    ]);
    expect(left).toBe(right);
    expect(await membershipsOf(account)).toHaveLength(1);

    // Across processes the exclusive transaction decides: bypass the dedupe.
    const racer = crypto.randomUUID();
    const [a, b] = await Promise.all([
      seedPersonalCanvas(db, racer, "Hedy"),
      seedPersonalCanvas(db, racer, "Hedy"),
    ]);
    expect(a).toBe(b);
    expect((await membershipsOf(racer)).map((row) => row.canvasId)).toEqual([a]);
  });
});

describe("redeemInvite", () => {
  async function adminWithInvite(role: "viewer" | "editor", singleUse: boolean) {
    const admin = crypto.randomUUID();
    const canvasId = await ensurePersonalCanvas(db, admin, "Host");
    const token = crypto.randomUUID();
    await db.insert(app.canvasInvites, { canvasId, token, role, singleUse }).wait({
      tier: "global",
    });
    return { admin, canvasId, token };
  }

  it("admits a new member once and stays idempotent", async () => {
    const { canvasId, token } = await adminWithInvite("editor", false);
    const guest = crypto.randomUUID();
    expect(await redeemInvite(db, guest, { canvasId, token })).toBe("joined");
    expect(await redeemInvite(db, guest, { canvasId, token })).toBe("already-member");
    const rows = await db.all(app.canvasMembers.where({ canvasId, memberAuthor: guest }), {
      tier: "global",
    });
    expect(rows.map((row) => row.role)).toEqual(["editor"]);
    // A multi-use link keeps working for the next person.
    expect(await redeemInvite(db, crypto.randomUUID(), { canvasId, token })).toBe("joined");
  });

  it("never downgrades an existing membership", async () => {
    const { admin, canvasId, token } = await adminWithInvite("viewer", false);
    expect(await redeemInvite(db, admin, { canvasId, token })).toBe("already-member");
    const rows = await db.all(app.canvasMembers.where({ canvasId, memberAuthor: admin }), {
      tier: "global",
    });
    expect(rows.map((row) => row.role)).toEqual(["admin"]);
  });

  it("consumes a single-use invite for exactly one of two racing redeemers", async () => {
    const { canvasId, token } = await adminWithInvite("editor", true);
    const results = await Promise.all([
      redeemInvite(db, crypto.randomUUID(), { canvasId, token }).catch(() => "rejected"),
      redeemInvite(db, crypto.randomUUID(), { canvasId, token }).catch(() => "rejected"),
    ]);
    expect(results.filter((result) => result === "joined")).toHaveLength(1);
    expect(await db.all(app.canvasInvites.where({ canvasId }), { tier: "global" })).toEqual([]);
  });

  it("rejects unknown tokens and tokens for another canvas", async () => {
    const { token } = await adminWithInvite("editor", false);
    const other = await ensurePersonalCanvas(db, crypto.randomUUID(), "Other");
    const guest = crypto.randomUUID();
    expect(await redeemInvite(db, guest, { canvasId: other, token })).toBe("invalid");
    expect(await redeemInvite(db, guest, { canvasId: other, token: crypto.randomUUID() })).toBe(
      "invalid",
    );
    expect(await membershipsOf(guest)).toEqual([]);
  });
});
