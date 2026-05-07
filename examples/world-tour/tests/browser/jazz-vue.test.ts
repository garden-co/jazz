/**
 * Browser tests for the world-tour Jazz + Vue integration.
 *
 * Mounts small Vue components inside a JazzProvider against a Jazz client
 * connected to the per-suite TestingServer (see global-setup.ts), then
 * exercises the schema through the public composables (useDb, useAll,
 * useSession). Each test scopes its data with a unique marker so the
 * shared server state from prior tests doesn't bleed into assertions.
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

function uniqueMarker(label: string): string {
  return `${label}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
}

describe("world-tour Jazz + Vue integration", () => {
  it("useDb insert is observed by useAll in another component", async () => {
    const marker = uniqueMarker("venue");

    const VenueList = defineComponent({
      setup() {
        const venues = useAll(app.venues.where({ name: marker }));
        return () =>
          h("ul", { id: "venues" }, venues.value?.map((v) => h("li", { key: v.id }, v.name)) ?? []);
      },
    });

    const { el, client } = await mount(VenueList);
    expect(el.querySelectorAll("#venues li").length).toBe(0);

    client.db.insert(app.venues, {
      name: marker,
      city: "London",
      country: "UK",
      lat: 51.5159,
      lng: -0.1311,
    });

    await waitFor(
      () => el.querySelectorAll("#venues li").length === 1,
      5000,
      "venue should appear after insert",
    );
    expect(el.querySelector("#venues li")!.textContent).toBe(marker);
  });

  it("useAll resolves .include() relations and reflects later updates", async () => {
    const marker = uniqueMarker("stop");

    const StopList = defineComponent({
      setup() {
        const stops = useAll(
          app.stops
            .where({ publicDescription: marker })
            .include({ venue: true })
            .orderBy("date", "asc"),
        );
        return () =>
          h(
            "ul",
            { id: "stops" },
            stops.value?.map((s) =>
              h(
                "li",
                { key: s.id, "data-status": s.status },
                `${s.venue?.name ?? "?"}: ${s.publicDescription}`,
              ),
            ) ?? [],
          );
      },
    });

    const { el, client } = await mount(StopList);

    const userId = client.session?.user_id;
    if (!userId) throw new Error("test session is missing user_id");

    const { value: band } = client.db.insert(app.bands, { name: `${marker}-band` });
    client.db.insert(app.members, { bandId: band.id, userId });
    const { value: venue } = client.db.insert(app.venues, {
      name: `${marker}-venue`,
      city: "London",
      country: "UK",
      lat: 51.5159,
      lng: -0.1311,
    });
    const { value: stop } = client.db.insert(app.stops, {
      bandId: band.id,
      venueId: venue.id,
      date: new Date("2026-08-01"),
      status: "confirmed",
      publicDescription: marker,
    });

    await waitFor(
      () => el.querySelectorAll("#stops li").length === 1,
      5000,
      "stop with included venue should appear",
    );
    const li = el.querySelector("#stops li")!;
    expect(li.textContent).toBe(`${marker}-venue: ${marker}`);
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
    const marker = uniqueMarker("inserter");

    const Inserter = defineComponent({
      setup() {
        const db = useDb();
        const venues = useAll(app.venues.where({ name: marker }));
        function add() {
          db.insert(app.venues, {
            name: marker,
            city: "London",
            country: "UK",
            lat: 51.4659,
            lng: -0.1149,
          });
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
