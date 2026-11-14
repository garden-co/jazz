import { describe, expect, it, vi } from "vitest";
import type { WasmSchema } from "../drivers/types.js";
import type { AppArtifact } from "./app-artifact.js";

const artifact: AppArtifact = {
  format: 1,
  current: "a".repeat(64),
  bundle: "b".repeat(64),
  schemas: [{ hash: "a".repeat(64), tables: {} }],
  migrations: [],
};

/** A fresh copy of the module, as a second bundle of jazz-tools loads it. */
async function freshCopy() {
  vi.resetModules();
  return import("./app-artifact.js");
}

const todos = () => ({ todos: { columns: [] } }) as unknown as WasmSchema;

function messageOf(run: () => void): string {
  try {
    run();
  } catch (error) {
    return (error as Error).message;
  }
  throw new Error("expected a refusal");
}

describe("an app's shipped jazz.app.json", () => {
  it("is read by another copy of jazz-tools than the one that defined the app", async () => {
    // Next's server build can load the module defining the app and the one
    // opening its store as separate copies.
    const defining = await freshCopy();
    const opening = await freshCopy();
    expect(opening).not.toBe(defining);

    const schema = todos();
    defining.shipArtifact(schema, { artifact });

    expect(opening.shippedArtifact(schema)).toBe(artifact);
  });

  it("leaves the schema's JSON, and so its hash, as it was", async () => {
    const { shipArtifact, shippedArtifact } = await freshCopy();
    const schema = todos();
    const before = JSON.stringify(schema);
    shipArtifact(schema, { artifact });
    expect(JSON.stringify(schema)).toBe(before);
    expect(Object.keys(schema)).toEqual(["todos"]);
    expect(shippedArtifact(schema)).toBe(artifact);
  });

  it('is none for an app on { migrations: "none" }', async () => {
    const { shipArtifact, shippedArtifact, shipsNoMigrations } = await freshCopy();
    const schema = todos();
    shipArtifact(schema, { migrations: "none" });
    expect(shippedArtifact(schema)).toBeUndefined();
    expect(shipsNoMigrations(schema)).toBe(true);
  });

  it("is required, and the refusal says how to ship it", async () => {
    const { shipArtifact } = await freshCopy();
    for (const options of [undefined, {}]) {
      const message = messageOf(() => shipArtifact(todos(), options as never));
      expect(message).toMatch(/^defineApp needs the app's jazz\.app\.json: /);
      expect(message).toContain('import artifact from "./jazz.app.json" with { type: "json" };');
      expect(message).toContain("s.defineApp(schema, { artifact })");
      expect(message).toContain("npx jazz-tools validate");
      expect(message).toContain('{ migrations: "none" }');
    }
  });

  it("refuses the alpha.62 { migrations } shape, saying how to move to jazz.app.json", async () => {
    const { shipArtifact } = await freshCopy();
    const legacy = { schemas: [], migrations: [] };
    for (const options of [{ migrations: legacy }, { artifact: { migrations: legacy } }]) {
      const message = messageOf(() => shipArtifact(todos(), options as never));
      expect(message).toMatch(/migrations\/index\.ts/);
      expect(message).toContain("npx jazz-tools validate");
      expect(message).toContain("commit the file");
      expect(message).toContain("s.defineApp(schema, { artifact })");
    }
  });

  it("refuses what isn't a jazz.app.json", async () => {
    const { shipArtifact } = await freshCopy();
    expect(
      messageOf(() => shipArtifact(todos(), { artifact: { default: artifact } as never })),
    ).toMatch(/module namespace/);
    expect(messageOf(() => shipArtifact(todos(), { artifact: { format: 1 } as never }))).toMatch(
      /not a jazz\.app\.json/,
    );
  });

  it("tells a shipped migration apart from a single version", async () => {
    const { shipsMigrations } = await freshCopy();
    expect(shipsMigrations(undefined)).toBe(false);
    expect(shipsMigrations(artifact)).toBe(false);
    expect(shipsMigrations({ ...artifact, migrations: [{}] })).toBe(true);
  });
});
