// Users and local-first guests on a Durable Object core: the app's backend
// (createJazz with remote()) serves its clients over HTTP, and its
// core is the real jazz-wasm host in local workerd. The same checks as
// guest-decision.integration.test.ts runs on the other stores.
import { randomUUID } from "node:crypto";
import { describe, expect, it, vi } from "vitest";
import { acceptDeviceAccounts } from "../../src/backend/index.js";
import { hostBuilt } from "./host.js";
import { app, inAnHour, setupTestApp } from "./test-app.js";

describe.skipIf(!hostBuilt && !process.env.CI)(
  "guests and signed-in users on a Durable Object",
  () => {
    const { appOn, signInAs } = setupTestApp("while-connected");

    it("syncs a device account (guest) the app accepts", async () => {
      const { jazz, client } = await appOn((request) => acceptDeviceAccounts(request));
      const guest = await client("device");
      const row = await guest.db
        .insert(app.todos, { title: "made as a guest", owner: guest.session!.user! })
        .wait({ tier: "global" });
      const seen = await jazz.db.all(app.todos.where({ id: row.id }));
      expect(seen.map((todo) => todo.title)).toEqual(["made as a guest"]);
    });

    it("keeps one guest's rows from another, by the app's permissions", async () => {
      const { client } = await appOn((request) => acceptDeviceAccounts(request));
      const first = await client("device");
      const second = await client("device");
      expect(first.session!.user).not.toBe(second.session!.user);
      await first.db
        .insert(app.todos, { title: "first's", owner: first.session!.user! })
        .wait({ tier: "global" });
      expect((await first.db.all(app.todos, { tier: "remote" })).map((t) => t.title)).toEqual([
        "first's",
      ]);
      expect(await second.db.all(app.todos, { tier: "remote" })).toEqual([]);
    });

    it("refuses a device account (guest) the app doesn't accept", async () => {
      const warn = vi.spyOn(console, "warn").mockImplementation(() => undefined);
      const { jazz, client } = await appOn(() => null);
      const guest = await client("device");
      void guest.db
        .insert(app.todos, { title: "made as a guest", owner: guest.session!.user! })
        .wait({ tier: "global" })
        .catch(() => undefined);
      await vi.waitFor(
        () =>
          expect(
            warn.mock.calls.some((call) => /refused a device account/.test(call.join(" "))),
          ).toBe(true),
        { timeout: 20_000 },
      );
      // Its write never reached the core.
      expect(await jazz.db.all(app.todos)).toEqual([]);
    });

    it("serves a signed-in user, the session stated by the backend", async () => {
      const { jazz, client, origin } = await appOn((request) => {
        const userId = request.headers.get("x-test-user");
        return userId ? { userId, expiresAt: inAnHour() } : acceptDeviceAccounts(request);
      });
      signInAs(origin, "alice");
      const alice = await client();
      expect(alice.session?.identity).toEqual({ issuer: "jazz:backend", subject: "alice" });
      await alice.db
        .insert(app.todos, { title: "alice's", owner: alice.session!.user! })
        .wait({ tier: "global" });
      const asAlice = await jazz.forReqUser(
        new Request("http://app/x", { headers: { "x-test-user": "alice" } }),
      );
      expect((await asAlice.all(app.todos)).map((todo) => todo.title)).toEqual(["alice's"]);
      // The app's permissions hold for her session: she sees only her rows,
      // and can't write one as someone else.
      await jazz.db
        .insert(app.todos, { title: "someone else's", owner: randomUUID() })
        .wait({ tier: "global" });
      expect((await jazz.db.all(app.todos)).length).toBe(2);
      expect((await alice.db.all(app.todos, { tier: "remote" })).map((todo) => todo.title)).toEqual(
        ["alice's"],
      );
      await expect(
        alice.db.insert(app.todos, { title: "as someone else", owner: randomUUID() }).wait({
          tier: "global",
        }),
      ).rejects.toThrow();
      expect((await jazz.db.all(app.todos)).map((todo) => todo.title).sort()).toEqual([
        "alice's",
        "someone else's",
      ]);
    });
  },
);
