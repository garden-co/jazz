/**
 * `point` columns end to end (SPEC 20.1): `[lng, lat]` in, the same bits
 * out through the native runtime, whole-point filters bit for bit, and
 * out-of-range points refused before they are written.
 */
import { afterEach, describe, expect, it } from "vitest";
import { testApp } from "../testing/test-app.js";
import { localAccountConfig } from "./testing/account-fixtures.js";
import type { Db } from "./db.js";
import { createDb } from "./default-create-db.js";

const app = testApp({
  tables: {
    places: {
      columns: {
        name: "string",
        location: "point",
        pickup: "point?",
        home: { type: "point", default: [13.405, 52.52] },
      },
    },
  },
});

describe("point columns", () => {
  let db: Db | undefined;
  afterEach(async () => {
    await db?.shutdown();
    db = undefined;
  });

  it("round-trips [lng, lat] bit for bit, defaults and nulls included", async () => {
    db = await createDb({
      ...(await localAccountConfig("point-columns-round-trip")),
      driver: { type: "memory" },
    });
    const { value: office } = db.insert(app.places, {
      name: "office",
      location: [-122.419416, 37.774929],
      pickup: null,
    });
    const { value: origin } = db.insert(app.places, {
      name: "origin",
      location: [-0, Number.MIN_VALUE],
      pickup: [180, -90],
    });
    const rows = new Map((await db.all(app.places)).map((row) => [row.id, row]));
    expect(rows.get(office.id)?.location).toEqual([-122.419416, 37.774929]);
    expect(rows.get(office.id)?.home).toEqual([13.405, 52.52]);
    expect(rows.get(office.id)?.pickup).toBeNull();
    const [lng, lat] = rows.get(origin.id)!.location as [number, number];
    expect(Object.is(lng, -0)).toBe(true);
    expect(lat).toBe(Number.MIN_VALUE);
    expect(rows.get(origin.id)?.pickup).toEqual([180, -90]);
  });

  it("filters whole points bit for bit: -0 and 0 differ", async () => {
    db = await createDb({
      ...(await localAccountConfig("point-columns-filters")),
      driver: { type: "memory" },
    });
    const { value: zero } = db.insert(app.places, { name: "zero", location: [0, 0], pickup: null });
    const { value: negZero } = db.insert(app.places, {
      name: "negative zero",
      location: [-0, 0],
      pickup: null,
    });
    const names = async (where: Record<string, unknown>) =>
      (await db!.all(app.places.where(where))).map((row) => row.id);
    expect(await names({ location: [0, 0] })).toEqual([zero.id]);
    expect(await names({ location: [-0, 0] })).toEqual([negZero.id]);
    expect(
      (
        await names({
          location: {
            in: [
              [0, 0],
              [-0, 0],
            ],
          },
        })
      ).sort(),
    ).toEqual([zero.id, negZero.id].sort());
    expect(await names({ location: { ne: [0, 0] } })).toEqual([negZero.id]);
  });

  it("refuses latitude-first and out-of-range points before writing", async () => {
    db = await createDb({
      ...(await localAccountConfig("point-columns-refusals")),
      driver: { type: "memory" },
    });
    // San Francisco written latitude first: latitude -122 is out of range.
    expect(() =>
      db!.insert(app.places, { name: "swapped", location: [37.774929, -122.419416], pickup: null }),
    ).toThrow(/GeoJSON order/);
    expect(() =>
      db!.insert(app.places, { name: "nan", location: [Number.NaN, 0], pickup: null }),
    ).toThrow(/longitude/);
    expect(() =>
      // @ts-expect-error a point is [lng, lat]: the type refuses one coordinate too
      db!.insert(app.places, { name: "short", location: [1], pickup: null }),
    ).toThrow(/two numbers/);
    expect(await db.all(app.places)).toEqual([]);
  });
});
