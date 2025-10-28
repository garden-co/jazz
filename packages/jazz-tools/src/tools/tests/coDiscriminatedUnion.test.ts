import { beforeEach, describe, expect, test, vi } from "vitest";
import { CoPlainText, Loaded, co, z } from "../exports.js";
import { createJazzTestAccount, setupJazzTestSync } from "../testing.js";
import { waitFor } from "./utils.js";

describe("co.discriminatedUnion", () => {
  beforeEach(async () => {
    await setupJazzTestSync();

    await createJazzTestAccount({
      isCurrentActiveAccount: true,
      creationProps: { name: "Hermes Puggington" },
    });
  });

  test("use co.discriminatedUnion with CoValue schemas as values", () => {
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

    expect(person.pet.type).toEqual("dog");

    person.$jazz.set(
      "pet",
      Cat.create({
        type: "cat",
      }),
    );

    expect(person.pet.type).toEqual("cat");
  });

  test("use nested co.discriminatedUnions", () => {
    const BaseError = { status: z.literal("failed"), message: z.string() };
    const BadRequestError = co.map({ ...BaseError, code: z.literal(400) });
    const UnauthorizedError = co.map({ ...BaseError, code: z.literal(401) });
    const InternalServerError = co.map({ ...BaseError, code: z.literal(500) });
    const Errors = co.discriminatedUnion("code", [
      BadRequestError,
      UnauthorizedError,
      InternalServerError,
    ]);

    const Success = co.map({ status: z.literal("success"), data: z.string() });
    const Response = co.map({
      result: co.discriminatedUnion("status", [Success, Errors]),
    });

    const response = Response.create({
      result: Success.create({
        status: "success",
        data: "Hello, world!",
      }),
    });

    expect(response.result.status).toEqual("success");
    if (response.result.status === "success") {
      expect(response.result.data).toEqual("Hello, world!");
    }

    response.$jazz.set(
      "result",
      BadRequestError.create({
        status: "failed",
        message: "Bad request",
        code: 400,
      }),
    );

    expect(response.result.status).toEqual("failed");
    if (response.result.status === "failed") {
      expect(response.result.code).toEqual(400);
      if (response.result.code === 400) {
        expect(response.result.message).toEqual("Bad request");
      }
    }
  });

  test("use deeply nested co.discriminatedUnions", () => {
    const BaseError = { status: z.literal("failed"), message: z.string() };
    const BadRequestError = co.map({ ...BaseError, code: z.literal(400) });
    const UnauthorizedError = co.map({ ...BaseError, code: z.literal(401) });
    const Errors = co.discriminatedUnion("code", [
      BadRequestError,
      co.discriminatedUnion("code", [
        co.discriminatedUnion("code", [
          co.discriminatedUnion("code", [UnauthorizedError]),
        ]),
      ]),
    ]);

    const Response = co.map({
      error: Errors,
    });

    const response = Response.create({
      error: BadRequestError.create({
        status: "failed",
        message: "Bad request",
        code: 400,
      }),
    });

    expect(response.error.status).toEqual("failed");
    if (response.error.status === "failed") {
      expect(response.error.code).toEqual(400);
      if (response.error.code === 400) {
        expect(response.error.message).toEqual("Bad request");
      }
    }

    response.$jazz.set(
      "error",
      UnauthorizedError.create({
        status: "failed",
        message: "Unauthorized",
        code: 401,
      }),
    );

    expect(response.error.status).toEqual("failed");
    if (response.error.status === "failed") {
      expect(response.error.code).toEqual(401);
      if (response.error.code === 401) {
        expect(response.error.message).toEqual("Unauthorized");
      }
    }
  });

  test("co.discriminatedUnion works when nested inside a co.list", () => {
    const Dog = co.map({
      type: z.literal("dog"),
    });
    const Cat = co.map({
      type: z.literal("cat"),
    });
    const Pet = co.discriminatedUnion("type", [Dog, Cat]);

    const Pets = co.list(Pet);

    const Person = co.map({
      pets: Pets,
    });

    const pets = Pets.create([
      Dog.create({
        type: "dog",
      }),
      Cat.create({
        type: "cat",
      }),
    ]);
    const person = Person.create({
      pets,
    });

    expect(person.pets[0]?.type).toEqual("dog");
    expect(person.pets[1]?.type).toEqual("cat");
  });

  test("co.discriminatedUnion works when used in a recursive reference", () => {
    const NoteItem = co.map({
      type: z.literal("note"),
      internal: z.boolean(),
      content: co.plainText(),
    });

    const AttachmentItem = co.map({
      type: z.literal("attachment"),
      internal: z.boolean(),
      content: co.fileStream(),
    });

    const ReferenceItem = co.map({
      type: z.literal("reference"),
      internal: z.boolean(),
      content: z.string(),

      get child(): co.DiscriminatedUnion<
        [typeof NoteItem, typeof AttachmentItem, typeof ReferenceItem]
      > {
        return ProjectContextItem;
      },
    });

    const ProjectContextItem = co.discriminatedUnion("type", [
      NoteItem,
      AttachmentItem,
      ReferenceItem,
    ]);

    const referenceItem = ReferenceItem.create({
      type: "reference",
      internal: false,
      content: "Hello",
      child: NoteItem.create({
        type: "note",
        internal: false,
        content: CoPlainText.create("Hello"),
      }),
    });

    expect(referenceItem.child.type).toEqual("note");
  });

  test("co.discriminatedUnion works when used inside another schema in a recursive reference", () => {
    const NoteItem = co.map({
      type: z.literal("note"),
      internal: z.boolean(),
      content: co.plainText(),
    });

    const AttachmentItem = co.map({
      type: z.literal("attachment"),
      internal: z.boolean(),
      content: co.fileStream(),
    });

    const ReferenceItem = co.map({
      type: z.literal("reference"),
      internal: z.boolean(),
      content: z.string(),

      get children(): co.List<
        co.DiscriminatedUnion<
          [typeof NoteItem, typeof AttachmentItem, typeof ReferenceItem]
        >
      > {
        return ProjectContextItems;
      },
    });

    const ProjectContextItem = co.discriminatedUnion("type", [
      NoteItem,
      AttachmentItem,
      ReferenceItem,
    ]);

    const ProjectContextItems = co.list(ProjectContextItem);

    const referenceItem = ReferenceItem.create({
      type: "reference",
      internal: false,
      content: "Hello",
      children: ProjectContextItems.create([
        NoteItem.create({
          type: "note",
          internal: false,
          content: CoPlainText.create("Hello"),
        }),
      ]),
    });

    expect(referenceItem.children[0]?.type).toEqual("note");
  });

  test("load CoValue instances using the DiscriminatedUnion schema", async () => {
    const Dog = co.map({
      type: z.literal("dog"),
    });
    const Cat = co.map({
      type: z.literal("cat"),
    });
    const Pet = co.discriminatedUnion("type", [Dog, Cat]);

    const dog = Dog.create({ type: "dog" });
    const loadedPet = await Pet.load(dog.$jazz.id);
    expect(loadedPet?.type).toEqual("dog");
  });

  test("subscribe to CoValue instances using the DiscriminatedUnion schema", async () => {
    const Person = co.map({
      name: z.string(),
    });
    const Dog = co.map({
      type: z.literal("dog"),
      name: z.string(),
      owner: Person,
    });
    const Cat = co.map({
      type: z.literal("cat"),
      name: z.string(),
    });
    const Pet = co.discriminatedUnion("type", [Dog, Cat]);

    const dog = Dog.create({
      type: "dog",
      name: "Rex",
      owner: Person.create({ name: "John Doe" }),
    });

    const updates: Loaded<typeof Pet>[] = [];
    const spy = vi.fn((pet) => updates.push(pet));

    Pet.subscribe(dog.$jazz.id, {}, (pet) => {
      expect(pet.type).toEqual("dog");
      spy(pet);
    });

    expect(spy).not.toHaveBeenCalled();

    await waitFor(() => expect(spy).toHaveBeenCalled());

    expect(spy).toHaveBeenCalledTimes(1);

    expect(updates[0]?.name).toEqual("Rex");
  });

  test("should work when one of the options has a dicriminated union field", async () => {
    const Collie = co.map({
      type: z.literal("collie"),
    });
    const BorderCollie = co.map({
      type: z.literal("border-collie"),
    });
    const Breed = co.discriminatedUnion("type", [Collie, BorderCollie]);

    const Dog = co.map({
      type: z.literal("dog"),
      breed: Breed,
    });

    const Animal = co.discriminatedUnion("type", [Dog]);

    const animal = Dog.create({
      type: "dog",
      breed: {
        type: "collie",
      },
    });

    const loadedAnimal = await Animal.load(animal.$jazz.id);

    expect(loadedAnimal?.breed?.type).toEqual("collie");
  });

  test("should work with a nested co.discriminatedUnion", async () => {
    const Collie = co.map({
      type: z.literal("collie"),
    });
    const BorderCollie = co.map({
      type: z.literal("border-collie"),
    });
    const Breed = co.discriminatedUnion("type", [Collie, BorderCollie]);

    const Dog = co.discriminatedUnion("type", [Breed]);

    const Animal = co.discriminatedUnion("type", [Dog]);

    const animal = Collie.create({
      type: "collie",
    });

    const loadedAnimal = await Animal.load(animal.$jazz.id);

    expect(loadedAnimal?.type).toEqual("collie");
  });
});
