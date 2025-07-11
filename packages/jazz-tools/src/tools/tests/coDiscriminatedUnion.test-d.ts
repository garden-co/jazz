import { beforeEach, describe, test } from "vitest";
import { Loaded, co, z } from "../exports.js";
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
