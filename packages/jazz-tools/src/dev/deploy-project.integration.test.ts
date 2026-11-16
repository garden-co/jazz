import { afterEach, expect, it, vi } from "vitest";
import { testApp } from "../testing/index.js";
import { boundApp } from "../typed-app.js";
import { deployApp } from "./catalogue.js";
import type { SchemaSourceInput } from "../schema-source.js";
import { fetchMigrationGraph } from "./migration-graph.js";
import { startLocalJazzServer, type LocalJazzServerHandle } from "./dev-server.js";
import type { DeploymentBody } from "./catalogue-api.js";

let server: LocalJazzServerHandle | undefined;
afterEach(async () => {
  vi.restoreAllMocks();
  await server?.stop();
  server = undefined;
});

it("validates all branches before writing, deploys the converged history, and sends only missing artifacts", async () => {
  server = await startLocalJazzServer({ inMemory: true });
  const options = { serverUrl: server.url, appId: server.appId, adminSecret: server.adminSecret };
  // Two branches off v1 merged in v3. Both keep v1's `a`; merging branches
  // that each add a column the other lacks is refused (not supported yet).
  const columns = [[], ["a"], ["a", "tmp"], ["a", "b"]];
  const tables = (index: number) => ({
    notes: {
      columns: Object.fromEntries(["title", ...columns[index]!].map((c) => [c, "string"])),
    },
  });
  const hashes = columns.map(
    (_, index) => boundApp(testApp({ tables: tables(index) }).wasmSchema)!.schema.current,
  );
  const history = {
    v0: { tables: tables(0) },
    v1: { tables: tables(1) },
    v2: { tables: tables(2) },
  };
  const add = (column: string) => ({ [column]: { add: "string", default: "" } });
  const toV1 = { from: "v0", to: "v1", migrate: { notes: add("a") } };
  const toV2 = { from: "v1", to: "v2", migrate: { notes: add("tmp") } };
  const deploy = (app: SchemaSourceInput, permissions: object = { tables: {} }) =>
    deployApp({ ...options, app, permissions: { format: 1, ...permissions } });

  expect((await deploy(testApp({ tables: tables(0) }))).changed).toBe(true);
  const initial = await fetchMigrationGraph(options);

  // v3 with no migration into it: refused, and the store is left as it was.
  // The app's loader refuses it now, before anything is sent (the server's
  // "disconnected roots" check used to be the one that caught it).
  expect(() => testApp({ tables: tables(3), history, migrations: [toV1, toV2] })).toThrow(
    /history\.v\d: no migration leads from it to the current `tables`/,
  );
  expect(await fetchMigrationGraph(options)).toEqual(initial);

  const app = testApp({
    tables: tables(3),
    history,
    migrations: [
      toV1,
      toV2,
      { from: "v1", migrate: { notes: add("b") } },
      {
        from: "v2",
        migrate: { notes: { tmp: { drop: "string", backwardsDefault: "" }, ...add("b") } },
      },
    ],
  });

  const requests: DeploymentBody[] = [];
  const fetch = globalThis.fetch;
  vi.spyOn(globalThis, "fetch").mockImplementation((input, init) => {
    if (init?.method === "POST") {
      expect(String(input).endsWith("/admin/bundles")).toBe(true);
      requests.push(JSON.parse(String(init.body)));
    }
    return fetch(input, init);
  });
  const result = await deploy(app);
  expect(result.published.schemas.sort()).toEqual(hashes.slice(1).sort());
  expect(result.published.migrations).toHaveLength(4);
  expect(requests).toHaveLength(1);
  expect(requests[0]!.schemas).toHaveLength(3);
  expect(requests[0]!.migrations).toHaveLength(4);
  const graph = await fetchMigrationGraph(options);
  expect(graph.schemas).toContain(hashes[3]);
  expect(graph.migrations).toHaveLength(4);

  // A permissions change publishes a new bundle, but no schema or migration.
  expect((await deploy(app, { tables: { notes: { read: "true" } } })).changed).toBe(true);
  expect(requests[1]).toMatchObject({ schemas: [], migrations: [] });
  expect((await deploy(app, { tables: { notes: { read: "true" } } })).changed).toBe(false);
  expect(requests[2]).toMatchObject({ schemas: [], migrations: [] });
  expect(await fetchMigrationGraph(options)).toEqual(graph);
}, 30_000);
