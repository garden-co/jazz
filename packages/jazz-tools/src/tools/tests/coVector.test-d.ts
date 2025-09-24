import { describe, expectTypeOf, test } from "vitest";
import { Group, co } from "../exports.js";
import { CoVectorSchema } from "../internal.js";

describe("CoVector types", () => {
  test("co.vector() • defines a correct CoVectorSchema", () => {
    type ExpectedType = CoVectorSchema;

    function matches(value: ExpectedType) {
      return value;
    }

    matches(co.vector(384));
  });

  test("co.vector().create() • creates a CoVector with Float32Array-like typing", () => {
    const embedding = co.vector(3).create([1, 2, 3]);

    type ExpectedType = Readonly<Float32Array>;

    function matches(value: ExpectedType) {
      return value;
    }

    matches(embedding);
  });

  test("CoVector instance • has the owner property", () => {
    const embedding = co.vector(3).create([1, 2, 3]);

    expectTypeOf(embedding.$jazz.owner).toEqualTypeOf<Group>();
  });

  test("setting a value via index access • is a type error", () => {
    const embedding = co.vector(3).create([1, 2, 3]);

    // @ts-expect-error: assignment to index should be disallowed
    embedding[0] = 6;
  });
});
