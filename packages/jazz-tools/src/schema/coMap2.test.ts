import { beforeEach, describe, expect, it } from "vitest";
import { co } from "../exports.js";
import { createJazzTestAccount } from "../testing.js";
import { CoMapSchema } from "./coMap.js";

beforeEach(async () => {
  await createJazzTestAccount({
    isCurrentActiveAccount: true,
  });
});

describe("CoMap2", () => {
  it("should create a CoMap with basic property access", () => {
    class MyCoMap extends CoMapSchema {
      name = co.string;
      age = co.number;
    }

    const myCoMap = MyCoMap.create({ name: "John", age: 30 });

    expect(myCoMap.name).toBe("John");
    expect(myCoMap.age).toBe(30);
  });

  it("should not change original property after calling $set", () => {
    class MyCoMap extends CoMapSchema {
      name = co.string;
      age = co.number;
    }

    const myCoMap = MyCoMap.create({ name: "John", age: 30 });
    myCoMap.$set("age", 31);

    expect(myCoMap.age).toBe(30);
    expect(Object.entries(myCoMap)).toEqual([
      ["name", "John"],
      ["age", 30],
    ]);
  });

  it("should properly serialize to JSON", () => {
    class MyCoMap extends CoMapSchema {
      name = co.string;
      age = co.number;
    }

    const myCoMap = MyCoMap.create({ name: "John", age: 30 });

    expect(JSON.stringify(myCoMap)).toMatchInlineSnapshot(
      `"{"name":"John","age":30}"`,
    );
  });

  it("should return updated values after calling $set and $updated", () => {
    class MyCoMap extends CoMapSchema {
      name = co.string;
      age = co.number;
    }

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
    class MyCoMap extends CoMapSchema {
      name = co.string;
      age = co.number;
    }

    const myCoMap = MyCoMap.create({ name: "John", age: 30 });

    expect(myCoMap.$updated()).toBe(myCoMap);
  });

  it("should create a Record-like CoMap with correct properties", () => {
    class MyCoMap extends CoMapSchema.Record(co.string) {}

    const myCoMap = MyCoMap.create({
      firstName: "John",
      lastName: "Doe",
    });

    expect(myCoMap.firstName).toBe("John");
    expect(myCoMap.lastName).toBe("Doe");

    expect(Object.entries(myCoMap)).toEqual([
      ["firstName", "John"],
      ["lastName", "Doe"],
    ]);
  });

  it("should update existing properties in a Record-like CoMap", () => {
    class MyCoMap extends CoMapSchema.Record(co.string) {}

    const myCoMap = MyCoMap.create({
      firstName: "John",
      lastName: "Doe",
    });

    myCoMap.$set("firstName", "Jane");
    const updated = myCoMap.$updated();

    expect(myCoMap.firstName).toBe("John");
    expect(updated.firstName).toBe("Jane");
    expect(updated.lastName).toBe("Doe");
  });

  it("should add new properties to a Record-like CoMap", () => {
    class MyCoMap extends CoMapSchema.Record(co.string) {}

    const myCoMap = MyCoMap.create({
      firstName: "John",
      lastName: "Doe",
    });

    myCoMap.$set("pronoun", "she");
    const updated = myCoMap.$updated();

    expect(updated.pronoun).toBe("she");

    expect(Object.entries(myCoMap)).toEqual([
      ["firstName", "John"],
      ["lastName", "Doe"],
    ]);

    expect(Object.entries(updated)).toEqual([
      ["firstName", "John"],
      ["lastName", "Doe"],
      ["pronoun", "she"],
    ]);
  });
});
