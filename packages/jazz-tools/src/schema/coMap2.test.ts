import { beforeEach, describe, expect, it } from "vitest";
import { createJazzTestAccount } from "../testing.js";
import { co } from "./schema.js";

beforeEach(async () => {
  await createJazzTestAccount({
    isCurrentActiveAccount: true,
  });
});

describe("CoMap2", () => {
  it("should create a CoMap with basic property access", () => {
    const MyCoMap = co.map({
      name: co.string(),
      age: co.number(),
    });

    const myCoMap = MyCoMap.create({ name: "John", age: 30 });

    expect(myCoMap.name).toBe("John");
    expect(myCoMap.age).toBe(30);
  });

  it("should create a CoMap with nested values", () => {
    const NestedCoMap = co.map({
      name: co.string(),
      age: co.number(),
    });

    const MyCoMap = co.map({
      name: co.string(),
      age: co.number(),
      ref: NestedCoMap,
    });

    const myCoMap = MyCoMap.create({
      name: "John",
      age: 30,
      ref: {
        name: "Jane",
        age: 20,
      },
    });

    expect(myCoMap.name).toBe("John");
    expect(myCoMap.age).toBe(30);
    expect(myCoMap.ref?.name).toBe("Jane");
    expect(myCoMap.ref?.age).toBe(20);
  });

  it("should not change original property after calling $set", () => {
    const MyCoMap = co.map({
      name: co.string(),
      age: co.number(),
    });

    const myCoMap = MyCoMap.create({ name: "John", age: 30 });
    myCoMap.$set("age", 31);

    expect(myCoMap.age).toBe(30);
    expect(Object.entries(myCoMap)).toEqual([
      ["name", "John"],
      ["age", 30],
    ]);
  });

  it("should properly serialize to JSON", () => {
    const MyCoMap = co.map({
      name: co.string(),
      age: co.number(),
    });

    const myCoMap = MyCoMap.create({ name: "John", age: 30 });

    expect(JSON.stringify(myCoMap)).toMatchInlineSnapshot(
      `"{"name":"John","age":30}"`,
    );
  });

  it("should properly serialize nedted values to JSON", () => {
    const NestedCoMap = co.map({
      name: co.string(),
      age: co.number(),
    });

    const MyCoMap = co.map({
      name: co.string(),
      age: co.number(),
      ref: NestedCoMap,
    });

    const myCoMap = MyCoMap.create({
      name: "John",
      age: 30,
      ref: NestedCoMap.create({ name: "Jane", age: 20 }),
    });

    expect(JSON.stringify(myCoMap)).toMatchInlineSnapshot(
      `"{"name":"John","age":30,"ref":{"name":"Jane","age":20}}"`,
    );
  });

  it("should return updated values after calling $set and $updated", () => {
    const MyCoMap = co.map({
      name: co.string(),
      age: co.number(),
    });

    const myCoMap = MyCoMap.create({ name: "John", age: 30 });
    myCoMap.$set("age", 31);

    const updated = myCoMap.$updated();

    expect(updated.age).toBe(31);
    expect(Object.entries(updated)).toEqual([
      ["name", "John"],
      ["age", 31],
    ]);
  });

  it("should return the same instance on $updated if there are no changes", () => {
    const MyCoMap = co.map({
      name: co.string(),
      age: co.number(),
    });

    const myCoMap = MyCoMap.create({ name: "John", age: 30 });

    expect(myCoMap.$updated()).toBe(myCoMap);
  });
});
