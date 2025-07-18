import { describe, test } from "vitest";
import { z } from "../exports";
import { co } from "../internal";

describe("CoValue and Zod schema compatibility", () => {
  test("cannot use z.record with CoValue schemas as values", () => {
    const Dog = co.map({
      type: z.literal("dog"),
    });

    const Person = co.map({
      // @ts-expect-error: cannot use z.record with a CoValue schema
      // (z.record is not exported by jazz-tools)
      pets: z.record(z.string(), Dog),
    });
  });

  test("cannot use z.union with CoValue schemas as values", () => {
    const Dog = co.map({
      type: z.literal("dog"),
    });

    const Cat = co.map({
      type: z.literal("cat"),
    });

    const Person = co.map({
      // @ts-expect-error: cannot use z.union with a CoValue schema
      pets: z.union([Dog, Cat]),
    });
  });

  test("cannot use z.intersection with CoValue schemas as values", () => {
    const Dog = co.map({
      type: z.literal("dog"),
    });

    const Cat = co.map({
      type: z.literal("cat"),
    });

    const Person = co.map({
      // @ts-expect-error: cannot use z.intersection with a CoValue schema
      // (z.intersection is not exported by jazz-tools)
      pets: z.intersection(Dog, Cat),
    });
  });

  test("cannot use z.array with CoValue schemas as values", () => {
    const Dog = co.map({
      type: z.literal("dog"),
    });

    const Person = co.map({
      // @ts-expect-error: cannot use z.array with a CoValue schema
      pets: z.array(Dog),
    });
  });

  test("cannot use z.tuple with CoValue schemas as values", () => {
    const Dog = co.map({
      type: z.literal("dog"),
    });

    const Person = co.map({
      // @ts-expect-error: cannot use z.tuple with a CoValue schema
      pets: z.tuple([Dog]),
    });
  });
});
