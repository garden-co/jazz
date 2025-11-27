import { co, CoPlainText } from "jazz-tools";
import { assertLoaded } from "jazz-tools/testing";
import { describe, expectTypeOf, test } from "vitest";
import { CoState } from "../jazz.class.svelte";

describe("CoState", () => {
  test("should use the schema's resolve query if no resolve query is provided", async () => {
    const Person = co.map({
      name: co.plainText(),
    });

    const PersonWithName = Person.resolved({ name: true });
    const person = Person.create({ name: "John Doe" });

    const loadedPerson = new CoState(PersonWithName, person.$jazz.id);

    assertLoaded(loadedPerson.current);
    expectTypeOf<typeof loadedPerson.current.name>().toEqualTypeOf<CoPlainText>();
  });
});
