/**
 * Browser tests for the world-tour Jazz + Vue integration.
 *
 * Mounts small Vue components inside a JazzProvider against a Jazz client
 * connected to the per-suite TestingServer (see global-setup.ts), then
 * exercises the schema through the public composables (useDb, useAll,
 * useSession).
 *
 * ## Isolation
 *
 * Vitest browser mode runs tests inside Chromium, where jazz-tools/testing
 * (Node-only) can't load — per-test fresh servers would require the vitest
 * `commands` IPC pattern. Instead each test calls `scope()` to get a unique
 * marker and a `queries` object whose `where()` filters are bound to it.
 * Inserts use `scope.tag(...)` to stamp every row, so even though all tests
 * share one server, every assertion only sees its own test's data.
 */
import { afterEach, describe, expect, it } from "vitest";
import { type App, createApp, defineComponent, h } from "vue";
import {
  type JazzClient,
  JazzProvider,
  createJazzClient,
  useAll,
  useDb,
  useSession,
} from "jazz-tools/vue";
import { app } from "../../schema.js";
import { APP_ID, TEST_PORT } from "./test-constants.js";

const SERVER_URL = `http://127.0.0.1:${TEST_PORT}`;

interface Scope {
  marker: string;
  /** Stamp a row payload's `name`/`publicDescription` field with the marker. */
  tag<T extends { name?: string; publicDescription?: string }>(payload: T): T;
  queries: {
    venues: ReturnType<typeof app.venues.where>;
    stops: ReturnType<typeof app.stops.where>;
  };
}

function scope(label: string): Scope {
  const marker = `${label}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
  return {
    marker,
    tag(payload) {
      const out = { ...payload };
      if ("name" in out) out.name = marker;
      if ("publicDescription" in out) out.publicDescription = marker;
      return out;
    },
    queries: {
      venues: app.venues.where({ name: marker }),
      stops: app.stops.where({ publicDescription: marker }),
    },
  };
}

/**
 * Await a `db.insert()` result up to edge-tier durability and return the
 * inserted value. Use this between dependent writes so the next op sees the
 * prior one — both for policy checks (`isBandMember`) and for relation
 * includes that need the related row to be visible at the server.
 */
async function inserted<T>(handle: {
  readonly value: T;
  wait(options: { tier: "local" | "edge" | "global" }): Promise<unknown>;
}): Promise<T> {
  await handle.wait({ tier: "edge" });
  return handle.value;
}

async function waitFor(check: () => boolean, ms: number, label: string): Promise<void> {
  const deadline = Date.now() + ms;
  while (Date.now() < deadline) {
    if (check()) return;
    await new Promise((r) => setTimeout(r, 30));
  }
  throw new Error(`Timeout waiting for: ${label}`);
}

interface Mounted {
  el: HTMLDivElement;
  client: JazzClient;
  vueApp: App;
}

const mounts: Mounted[] = [];

afterEach(async () => {
  while (mounts.length > 0) {
    const m = mounts.pop()!;
    m.vueApp.unmount();
    m.el.remove();
    await m.client.shutdown();
  }
});

async function mount(child: ReturnType<typeof defineComponent>): Promise<Mounted> {
  const client = await createJazzClient({
    appId: APP_ID,
    serverUrl: SERVER_URL,
    driver: { type: "memory" },
  });

  const el = document.createElement("div");
  document.body.appendChild(el);

  const Root = defineComponent({
    setup() {
      return () =>
        h(
          JazzProvider,
          { client },
          {
            default: () => h(child),
            fallback: () => h("p", { id: "jazz-loading" }, "loading"),
          },
        );
    },
  });

  const vueApp = createApp(Root);
  vueApp.mount(el);

  await waitFor(
    () => el.querySelector("#jazz-loading") === null,
    5000,
    "JazzProvider should resolve and render its default slot",
  );

  const m = { el, client, vueApp };
  mounts.push(m);
  return m;
}

describe("world-tour Jazz + Vue integration", () => {
  it("useDb insert is observed by useAll in another component", async () => {
    const s = scope("venue");
    const VenueList = defineComponent({
      setup() {
        const venues = useAll(s.queries.venues);
        return () =>
          h("ul", { id: "venues" }, venues.value?.map((v) => h("li", { key: v.id }, v.name)) ?? []);
      },
    });

    const { el, client } = await mount(VenueList);
    expect(el.querySelectorAll("#venues li").length).toBe(0);

    client.db.insert(
      app.venues,
      s.tag({
        name: "",
        city: "London",
        country: "UK",
        lat: 51.5159,
        lng: -0.1311,
      }),
    );

    await waitFor(
      () => el.querySelectorAll("#venues li").length === 1,
      5000,
      "venue should appear after insert",
    );
    expect(el.querySelector("#venues li")!.textContent).toBe(s.marker);
  });

  it("useAll resolves .include() relations and reflects later updates", async () => {
    const s = scope("stop");
    const StopList = defineComponent({
      setup() {
        const stops = useAll(s.queries.stops.include({ venue: true }).orderBy("date", "asc"));
        return () =>
          h(
            "ul",
            { id: "stops" },
            stops.value?.map((stop) =>
              h(
                "li",
                { key: stop.id, "data-status": stop.status },
                `${stop.venue?.name ?? "?"}: ${stop.publicDescription}`,
              ),
            ) ?? [],
          );
      },
    });

    const { el, client } = await mount(StopList);

    const userId = client.session?.user_id;
    if (!userId) throw new Error("test session is missing user_id");

    // Each dependent insert awaits edge-tier confirmation. The stop's policy
    // check (isBandMember) and the include-resolution of `venue` both require
    // the prior writes to be visible at the server before the next op lands.
    const band = await inserted(client.db.insert(app.bands, { name: `${s.marker}-band` }));
    await inserted(client.db.insert(app.members, { bandId: band.id, userId }));
    const venue = await inserted(
      client.db.insert(app.venues, {
        name: `${s.marker}-venue`,
        city: "London",
        country: "UK",
        lat: 51.5159,
        lng: -0.1311,
      }),
    );
    const stop = await inserted(
      client.db.insert(
        app.stops,
        s.tag({
          bandId: band.id,
          venueId: venue.id,
          date: new Date("2026-08-01"),
          status: "confirmed",
          publicDescription: "",
        }),
      ),
    );

    await waitFor(
      () => el.querySelectorAll("#stops li").length === 1,
      5000,
      "stop with included venue should appear",
    );
    const li = el.querySelector("#stops li")!;
    expect(li.textContent).toBe(`${s.marker}-venue: ${s.marker}`);
    expect(li.getAttribute("data-status")).toBe("confirmed");

    client.db.update(app.stops, stop.id, { status: "tentative" });

    await waitFor(
      () => el.querySelector("#stops li")!.getAttribute("data-status") === "tentative",
      5000,
      "status update should propagate to the rendered DOM",
    );
  });

  it("useSession exposes the JazzProvider's session to descendants", async () => {
    const SessionProbe = defineComponent({
      setup() {
        const session = useSession();
        return () => h("p", { id: "session" }, session ? `id:${session.user_id}` : "anonymous");
      },
    });

    const { el, client } = await mount(SessionProbe);
    const text = el.querySelector("#session")!.textContent ?? "";

    if (client.session) {
      expect(text).toBe(`id:${client.session.user_id}`);
    } else {
      expect(text).toBe("anonymous");
    }
  });

  it("useDb-driven insert from inside a child re-renders sibling useAll", async () => {
    const s = scope("inserter");
    const Inserter = defineComponent({
      setup() {
        const db = useDb();
        const venues = useAll(s.queries.venues);
        function add() {
          db.insert(
            app.venues,
            s.tag({
              name: "",
              city: "London",
              country: "UK",
              lat: 51.4659,
              lng: -0.1149,
            }),
          );
        }
        return () =>
          h("div", null, [
            h("button", { id: "add", onClick: add }, "add"),
            h("span", { id: "count" }, String(venues.value?.length ?? 0)),
          ]);
      },
    });

    const { el } = await mount(Inserter);
    expect(el.querySelector("#count")!.textContent).toBe("0");

    el.querySelector<HTMLButtonElement>("#add")!.click();

    await waitFor(
      () => el.querySelector("#count")!.textContent === "1",
      5000,
      "insert via useDb should bump the useAll count",
    );
  });
});
