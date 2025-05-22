import { beforeEach, describe, expect, test } from "vitest";
import { z } from "zod/v4";
import { co } from "../internal";
import { setupJazzTestSync } from "../testing";
import { createJazzTestAccount } from "../testing";

beforeEach(async () => {
  await setupJazzTestSync();

  await createJazzTestAccount({
    isCurrentActiveAccount: true,
    creationProps: { name: "Hermes Puggington" },
  });
});

describe("zodSchemas", () => {
  test("helper functions", async () => {
    const Pet = co.map({
      name: z.string(),
    });

    const Person = co
      .map({
        name: z.string(),
        age: z.number(),
        pet: Pet,
      })
      .withHelpers(() => ({
        createWithNameAndAge(name: string, age: number) {
          // should not cause circularity issues
          return Person.create({
            name,
            age,
            pet: Pet.create({ name: "Fido" }),
          });
        },
        createDefault() {
          // can use other helpers
          return Person.createWithNameAndAge("John", 20);
        },
      }))

      .withInstanceHelper(
        "nameAndAge",
        true,
        /** test docstring */
        function () {
          return `${this.name} is ${this.age} years old`;
        },
      )
      .withInstanceHelper("withPetName", { pet: true }, function () {
        return `${this.name} has a pet named ${this.pet.name}`;
      });

    const person = Person.createDefault();

    expect(person.nameAndAge()).toBe("John is 20 years old");
    expect(person.withPetName()).toBe("John has a pet named Fido");

    const person2 = await Person.load(person.id);

    if (!person2) {
      throw new Error("person2 is null");
    }

    expect(person2.nameAndAge()).toBe("John is 20 years old");

    expect(person2.withPetName()).toThrow("");
  });
});
