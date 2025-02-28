import { beforeEach, describe, expect, it } from "vitest";
import { co } from "../exports.js";
import { createJazzTestAccount } from "../testing.js";
import { CoMapInit, CoMapSchema } from "./coMap.js";

beforeEach(async () => {
  await createJazzTestAccount({
    isCurrentActiveAccount: true,
  });
});

describe("CoMap2", () => {
  it("should create a CoMap", () => {
    class MyCoMap extends CoMapSchema {
      name = co.string;
      age = co.number;
    }

    const myCoMap = MyCoMap.create({ name: "John", age: 30 });

    expect(myCoMap.name).toBe("John");
    expect(myCoMap.age).toBe(30);

    myCoMap.age = 31;

    expect(Object.entries(myCoMap)).toEqual([
      ["name", "John"],
      ["age", 31],
    ]);

    expect(myCoMap.age).toBe(31);
  });

  it("should create a Record-like CoMap", () => {
    class MyCoMap extends CoMapSchema.Record(co.string) {}

    const myCoMap = MyCoMap.create({
      firstName: "John",
      lastName: "Doe",
    });

    expect(myCoMap.firstName).toBe("John");
    expect(myCoMap.lastName).toBe("Doe");

    myCoMap.firstName = "Jane";
    myCoMap.$raw.set("pronoun", "she");

    expect(myCoMap.firstName).toBe("Jane");
    expect(myCoMap.lastName).toBe("Doe");

    // expect(Object.entries(myCoMap)).toEqual([
    //   ["firstName", "Jane"],
    //   ["lastName", "Doe"],
    //   ["pronoun", "she"],
    // ]);
  });
});
