import { afterEach, describe, expect, it, vi } from "vitest";
import {
  fetchMigrationGraph,
  combineMigrationGraphs,
  renderMigrationGraph,
  type MigrationGraph,
} from "./migration-graph.js";

const [a, b, c, d] = ["a", "b", "c", "d"].map((char) => char.repeat(64)) as [
  string,
  string,
  string,
  string,
];
const graph = (
  schemas: string[],
  pairs: [string, string][],
  activeSchemaHash: string | null = null,
): MigrationGraph => ({
  schemas,
  migrations: pairs.map(([fromHash, toHash]) => ({ fromHash, toHash })),
  activeSchemaHash,
});
afterEach(() => vi.unstubAllGlobals());

describe("migration graph", () => {
  it("renders a linear history and highlights the active schema, even when it is not terminal", () => {
    const history = graph(
      [c, a, b],
      [
        [b, c],
        [a, b],
      ],
      b,
    );
    expect(renderMigrationGraph(history)).toBe(
      ["● aaaaaaaaaaaa", "│", "▼", "● bbbbbbbbbbbb (current)", "│", "▼", "● cccccccccccc"].join(
        "\n",
      ),
    );
    expect(renderMigrationGraph(history, true)).toContain(
      "\x1b[1;36mbbbbbbbbbbbb (current)\x1b[0m",
    );
  });

  it("shows forks and marks repeated references to a converging schema", () => {
    expect(
      renderMigrationGraph(
        graph(
          [d, c, b, a],
          [
            [c, d],
            [a, c],
            [b, d],
            [a, b],
          ],
          d,
        ),
      ),
    ).toBe(
      [
        "● aaaaaaaaaaaa",
        "├─▶ ● bbbbbbbbbbbb",
        "│   │",
        "│   ▼",
        "│   ● dddddddddddd (current)",
        "└─▶ ● cccccccccccc",
        "    │",
        "    ▼",
        "    ● dddddddddddd (current) (shown above)",
        "",
        "Repeated labels refer to the same schema.",
      ].join("\n"),
    );
  });

  it("includes isolated schemas and terminates on historical cycles", () => {
    const text = renderMigrationGraph(
      graph(
        [a, b, c],
        [
          [a, b],
          [b, a],
        ],
      ),
    );
    expect(text).toContain("● cccccccccccc");
    expect(text.match(/aaaaaaaaaaaa/g)).toHaveLength(2);
    expect(text.match(/bbbbbbbbbbbb/g)).toHaveLength(1);
    expect(text).toContain("● aaaaaaaaaaaa (shown above)");
    expect(text).toContain("No schema is currently active.");
    expect(text.split("\n").length).toBeLessThan(20);
  });

  it.each([
    {
      name: "three branches",
      references: [],
      pairs: [
        [a, b],
        [a, c],
        [a, d],
      ],
    },
    {
      name: "multiple roots converging",
      references: [c],
      pairs: [
        [a, c],
        [b, c],
        [c, d],
      ],
    },
    {
      name: "a direct path alongside a longer path",
      references: [c],
      pairs: [
        [a, b],
        [b, c],
        [a, c],
      ],
    },
    {
      name: "a self migration alongside a forward migration",
      references: [a],
      pairs: [
        [a, a],
        [a, b],
      ],
    },
    {
      name: "a longer cycle",
      references: [a],
      pairs: [
        [a, b],
        [b, c],
        [c, a],
      ],
    },
  ])("expands every schema once and renders every edge for $name", ({ pairs, references }) => {
    const history = graph([...new Set(pairs.flat())], pairs as [string, string][], b);
    const text = renderMigrationGraph(history);
    for (const hash of history.schemas) {
      const rows = text.split("\n").filter((line) => line.includes(hash.slice(0, 12)));
      expect(rows.filter((line) => !line.endsWith("(shown above)"))).toHaveLength(1);
      expect(rows.filter((line) => line.endsWith("(shown above)"))).toHaveLength(
        references.includes(hash) ? 1 : 0,
      );
    }
    expect(text.match(/[▼▶]/g)).toHaveLength(pairs.length);
    expect(
      renderMigrationGraph({
        ...history,
        schemas: [...history.schemas].reverse(),
        migrations: [...history.migrations].reverse(),
      }),
    ).toBe(text);
  });

  it("distinguishes colliding short hashes and handles empty catalogues", () => {
    const collision = "a".repeat(12) + "b".repeat(52);
    const text = renderMigrationGraph(graph([a, collision], [], a));
    expect(text).toContain("aaaaaaaaaaaaa (current)");
    expect(text).toContain("aaaaaaaaaaaab");
    expect(renderMigrationGraph(graph([], []))).toBe("No local or server schemas found.");
  });

  it("marks schemas and migrations independently when combining local and server history", () => {
    const combined = combineMigrationGraphs(
      graph(
        [a, b, c],
        [
          [a, b],
          [b, c],
        ],
        b,
      ),
      {
        schemas: [a, b, d],
        migrations: [
          { fromHash: a, toHash: b },
          { fromHash: b, toHash: d },
          { fromHash: c, toHash: d },
        ],
        currentSchemaHash: d,
      },
    );
    expect(combined.schemaPresence).toEqual(
      new Map([
        [a, "both"],
        [b, "both"],
        [c, "server"],
        [d, "local"],
      ]),
    );
    expect(combined.migrations).toEqual(
      expect.arrayContaining([
        { fromHash: a, toHash: b, presence: "both" },
        { fromHash: b, toHash: c, presence: "server" },
        { fromHash: b, toHash: d, presence: "local" },
        { fromHash: c, toHash: d, presence: "local" },
      ]),
    );
    const text = renderMigrationGraph(combined);
    expect(text).toContain("bbbbbbbbbbbb (current)");
    expect(text).toContain("cccccccccccc [server only]");
    expect(text).toContain("dddddddddddd (schema.ts) [local only]");
    expect(text).toContain("├─▶ [server only] ● cccccccccccc [server only]");
    expect(text).toContain(
      "└─▶ [local only] ● dddddddddddd (schema.ts) [local only] (shown above)",
    );
    expect(text).toContain("│ [local only]");
  });

  it("fetches the app-scoped graph with admin authentication, preserving a server URL prefix", async () => {
    const history = graph([a], [], a);
    const fetch = vi.fn().mockResolvedValue(Response.json(history));
    vi.stubGlobal("fetch", fetch);
    await expect(
      fetchMigrationGraph({
        appId: "my app",
        serverUrl: "https://example.test/prefix/",
        adminSecret: "secret",
      }),
    ).resolves.toEqual(history);
    expect(fetch).toHaveBeenCalledWith(
      "https://example.test/prefix/apps/my%20app/admin/migrations/graph",
      {
        headers: { "X-Jazz-Admin-Secret": "secret" },
      },
    );
  });

  it("reports HTTP failures and malformed graphs instead of drawing misleading output", async () => {
    const fetch = vi
      .fn()
      .mockResolvedValueOnce(new Response("denied", { status: 401 }))
      .mockResolvedValueOnce(Response.json(graph([a], [[a, b]], a)))
      .mockResolvedValueOnce(Response.json({}));
    vi.stubGlobal("fetch", fetch);
    const options = { appId: "app", serverUrl: "https://example.test", adminSecret: "secret" };
    await expect(fetchMigrationGraph(options)).rejects.toThrow("Migration graph fetch failed: 401");
    await expect(fetchMigrationGraph(options)).rejects.toThrow("Invalid migration graph response");
    await expect(fetchMigrationGraph(options)).rejects.toThrow("Invalid migration graph response");
  });
});

it("labels inferred identity connections without suggesting a missing local migration", () => {
  const merged = combineMigrationGraphs(
    {
      schemas: [a, b],
      activeSchemaHash: b,
      migrations: [{ fromHash: a, toHash: b, automatic: true }],
    },
    { schemas: [a, b], currentSchemaHash: b, migrations: [] },
  );
  expect(renderMigrationGraph(merged)).toBe(
    ["● aaaaaaaaaaaa", "│ [automatic]", "▼", "● bbbbbbbbbbbb (current) (schema.ts)"].join("\n"),
  );
});
