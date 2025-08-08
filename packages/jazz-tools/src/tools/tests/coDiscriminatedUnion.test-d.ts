import { beforeEach, describe, test } from "vitest";
import { co, Loaded, z } from "../exports.js";
import { createJazzTestAccount, setupJazzTestSync } from "../testing.js";

describe("co.discriminatedUnion", () => {
  beforeEach(async () => {
    await setupJazzTestSync();

    await createJazzTestAccount({
      isCurrentActiveAccount: true,
      creationProps: { name: "Hermes Puggington" },
    });
  });

  test("can use co.discriminatedUnion with CoValue schemas as values", () => {
    const Dog = co.map({
      type: z.literal("dog"),
    });
    const Cat = co.map({
      type: z.literal("cat"),
    });
    const Person = co.map({
      pet: co.discriminatedUnion("type", [Dog, Cat]),
    });

    const person = Person.create({
      pet: Dog.create({
        type: "dog",
      }),
    });

    person.pet = Cat.create({
      type: "cat",
    });

    type ExpectedType = {
      pet: Loaded<typeof Dog> | Loaded<typeof Cat>;
    };

    function matches(value: ExpectedType) {
      return value;
    }

    matches(person);
  });

  test("recursive co.discriminatedUnion", () => {
    const Dog = co.map({
      type: z.literal("dog"),
      get friend() {
        return co.optional(Pet);
      },
    });
    const Cat = co.map({
      type: z.literal("cat"),
      get friend() {
        return co.optional(Pet);
      },
    });

    const Pet = co.discriminatedUnion("type", [Dog, Cat]);

    const pallino = Cat.create({
      type: "cat",
      friend: Dog.create({
        type: "dog",
      }),
    });

    pallino.friend = Cat.create({
      type: "cat",
    });

    type ExpectedType = {
      friend: Loaded<typeof Dog> | Loaded<typeof Cat> | undefined;
    };

    function matches(value: ExpectedType) {
      return value;
    }

    matches(pallino);
  });

  test("cannot use co.discriminatedUnion with zod schemas as values", () => {
    const Person = co.map({
      pet: co.discriminatedUnion("type", [
        // @ts-expect-error: cannot use co.discriminatedUnion with a Zod schema
        z.object({
          type: z.string(),
        }),
      ]),
    });
  });
});
