/**
 * `vector` columns filter whole (SPEC 20.1): `eq`, `ne`, `in` and `notIn`
 * take a `Float32Array` or an array of numbers alike, typed and at run time.
 */
import { afterEach, describe, expect, it } from "vitest";
import { testApp } from "../testing/test-app.js";
import { localAccountConfig } from "./testing/account-fixtures.js";
import type { Db } from "./db.js";
import { createDb } from "./default-create-db.js";

const app = testApp({
  tables: {
    items: {
      columns: {
        name: "string",
        embedding: { vector: 2 },
      },
    },
  },
});

describe("vector where", () => {
  let db: Db | undefined;
  afterEach(async () => {
    await db?.shutdown();
    db = undefined;
  });

  it("takes number[] as well as Float32Array for eq, ne, in and notIn", async () => {
    db = await createDb({
      ...(await localAccountConfig("vector-where")),
      driver: { type: "memory" },
    });
    const { value: a } = db.insert(app.items, { name: "a", embedding: new Float32Array([1, 0]) });
    const { value: b } = db.insert(app.items, { name: "b", embedding: new Float32Array([0, 1]) });
    const ids = async (query: ReturnType<typeof app.items.where>) =>
      (await db!.all(query)).map((row) => row.id);
    expect(await ids(app.items.where({ embedding: [1, 0] }))).toEqual([a.id]);
    expect(await ids(app.items.where({ embedding: { eq: [1, 0] } }))).toEqual([a.id]);
    expect(await ids(app.items.where({ embedding: { eq: new Float32Array([1, 0]) } }))).toEqual([
      a.id,
    ]);
    expect(await ids(app.items.where({ embedding: { ne: [1, 0] } }))).toEqual([b.id]);
    expect(await ids(app.items.where({ embedding: { in: [[0, 1]] } }))).toEqual([b.id]);
    expect(
      await ids(app.items.where({ embedding: { notIn: [new Float32Array([0, 1])] } })),
    ).toEqual([a.id]);
  });
});
