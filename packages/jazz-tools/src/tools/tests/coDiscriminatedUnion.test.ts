import { assert, beforeEach, describe, expect, test, vi } from "vitest";
import {
  CoPlainText,
  CoValueLoadingState,
  Group,
  Loaded,
  co,
  loadCoValue,
  z,
} from "../exports.js";
import { createJazzTestAccount, setupJazzTestSync } from "../testing.js";
import { assertLoaded, expectValidationError, waitFor } from "./utils.js";
import type { Account } from "jazz-tools";

describe("co.discriminatedUnion", () => {
  let account: Account;

  beforeEach(async () => {
    await setupJazzTestSync();

    account = await createJazzTestAccount({
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

  test("load CoValue instances using the DiscriminatedUnion schema without resolve", async () => {
    const Dog = co.map({
      type: z.literal("dog"),
    });
    const Cat = co.map({
      type: z.literal("cat"),
    });
    const Pet = co.discriminatedUnion("type", [Dog, Cat]);

    const dog = Dog.create({ type: "dog" });
    const loadedPet = await Pet.load(dog.$jazz.id);
    assertLoaded(loadedPet);
    expect(loadedPet.type).toEqual("dog");
  });

  test("load CoValue instances using the DiscriminatedUnion schema with deep resolve", async () => {
    const Person = co.map({
      name: z.string(),
    });
    const Dog = co.map({
      type: z.literal("dog"),
      owner: Person,
    });
    const Cat = co.map({
      type: z.literal("cat"),
      owner: Person,
    });
    const Pet = co.discriminatedUnion("type", [Dog, Cat]);

    const dog = Dog.create({
      type: "dog",
      owner: Person.create({
        name: "John Doe",
      }),
    });

    const loadedPet = await Pet.load(dog.$jazz.id, {
      resolve: {
        owner: true,
      },
    });

    assertLoaded(loadedPet);

    expect(loadedPet?.type).toEqual("dog");
    expect(loadedPet?.owner.name).toEqual("John Doe");
  });

  test("subscribe to CoValue instances using the DiscriminatedUnion schema without resolve", async () => {
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

  test("subscribe to CoValue instances using the DiscriminatedUnion schema with deep resolve", async () => {
    const Person = co.map({
      name: z.string(),
    });
    const Dog = co.map({
      type: z.literal("dog"),
      owner: Person,
    });
    const Cat = co.map({
      type: z.literal("cat"),
      owner: Person,
    });
    const Pet = co.discriminatedUnion("type", [Dog, Cat]);

    const dog = Dog.create({
      type: "dog",
      owner: Person.create({
        name: "John Doe",
      }),
    });

    const spy = vi.fn();
    Pet.subscribe(dog.$jazz.id, { resolve: { owner: true } }, (pet) => {
      expect(pet.owner.name).toEqual("John Doe");
      spy(pet);
    });

    expect(spy).not.toHaveBeenCalled();

    await waitFor(() => expect(spy).toHaveBeenCalled());

    expect(spy).toHaveBeenCalledTimes(1);
  });

  test("should work when one of the options has a discriminated union field", async () => {
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

    assertLoaded(loadedAnimal);
    assertLoaded(loadedAnimal.breed);
    expect(loadedAnimal.breed.type).toEqual("collie");
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

    assertLoaded(loadedAnimal);
    expect(loadedAnimal.type).toEqual("collie");
  });

  test("load co.discriminatedUnion with deep resolve using loadCoValue", async () => {
    const Person = co.map({
      name: z.string(),
    });
    const Dog = co.map({
      type: z.literal("dog"),
      owner: Person,
    });
    const Cat = co.map({
      type: z.literal("cat"),
    });

    const Pet = co.discriminatedUnion("type", [Dog, Cat]);

    const dog = Dog.create({
      type: "dog",
      owner: Person.create({ name: "John Doe" }),
    });

    const loadedPet = await loadCoValue(Pet.getCoValueClass(), dog.$jazz.id, {
      resolve: { owner: true },
      loadAs: account,
    });

    assertLoaded(loadedPet);

    if (loadedPet.type === "dog") {
      expect(loadedPet.owner.name).toEqual("John Doe");
    }
  });

  test("load co.discriminatedUnion with non-matching deep resolve", async () => {
    const Person = co.map({
      name: z.string(),
    });
    const Dog = co.map({
      type: z.literal("dog"),
      owner: Person,
    });
    const Cat = co.map({
      type: z.literal("cat"),
    });
    const Pet = co.discriminatedUnion("type", [Dog, Cat]);

    const cat = Cat.create({
      type: "cat",
    });

    const loadedPet = await Pet.load(cat.$jazz.id, {
      resolve: { owner: true },
    });

    assertLoaded(loadedPet);

    expect(loadedPet.type).toEqual("cat");
    // @ts-expect-error - no owner on Cat
    expect(loadedPet.owner).toBeUndefined();
  });

  test("load co.discriminatedUnion list with different schemas on deep resolved fields", async () => {
    // Schema without nested CoValues
    const Bird = co.map({
      type: z.literal("bird"),
      species: z.string(),
    });
    const Person = co.map({
      name: z.string(),
      bird: Bird,
    });
    // Schema with a nested CoValue
    const Dog = co.map({
      type: z.literal("dog"),
      friend: Person,
    });
    // Same attribute (friend) with a completely different schema (list)
    const Cat = co.map({
      type: z.literal("cat"),
      get friend() {
        return co.list(Cat);
      },
    });
    const Pet = co.discriminatedUnion("type", [Dog, Cat, Bird]);
    const Pets = co.list(Pet);

    const bird = Bird.create({
      type: "bird",
      species: "Parrot",
    });

    const dog = Dog.create({
      type: "dog",
      friend: Person.create({ name: "John Doe", bird }),
    });

    const cat = Cat.create({
      type: "cat",
      friend: [{ type: "cat", friend: [{ type: "cat", friend: [] }] }],
    });

    const pets = Pets.create([dog, cat, bird]);

    const loadedPets = await Pets.load(pets.$jazz.id, {
      resolve: { $each: { friend: { $each: { friend: true }, bird: true } } },
    });

    assertLoaded(loadedPets);

    for (const pet of loadedPets) {
      if (pet.type === "dog") {
        expect(pet.friend.name).toEqual("John Doe");
        expect(pet.friend.bird.species).toEqual("Parrot");
        // @ts-expect-error - no species on Person
        expect(pet.friend.species).toBeUndefined();
      } else if (pet.type === "cat") {
        expect(pet.friend[0]?.type).toEqual("cat");
        // @ts-expect-error - no name on Bird
        expect(pet.friend.name).toBeUndefined();
      }
    }
  });

  test("ensureLoaded on co.discriminatedUnion members", async () => {
    const Person = co.map({
      name: z.string(),
    });
    const Dog = co.map({
      type: z.literal("dog"),
      owner: Person,
    });
    const Cat = co.map({
      type: z.literal("cat"),
      friend: Person,
    });
    const Pet = co.discriminatedUnion("type", [Dog, Cat]);

    const dog = Dog.create({
      type: "dog",
      owner: Person.create({ name: "John Doe" }),
    });

    const cat = Cat.create({
      type: "cat",
      friend: Person.create({ name: "Jane Doe" }),
    });

    const pet = await Pet.load(dog.$jazz.id);

    assertLoaded(pet);

    // @ts-expect-error - can't use ensureLoaded before narrowing
    pet.$jazz.ensureLoaded({
      resolve: { owner: true },
    });

    if (pet.type === "dog") {
      const loadedPet = await pet.$jazz.ensureLoaded({
        resolve: { owner: true },
      });

      expect(loadedPet.owner.name).toEqual("John Doe");
    }
  });

  describe("Deep loading mutually exclusive nested CoMaps", async () => {
    const Breed = co.map({
      type: z.enum(["collie", "border-collie"]),
    });
    const Dog = co.map({
      type: z.literal("dog"),
      breed: Breed,
    });

    const Ocean = co.map({
      name: z.enum(["atlantic", "pacific"]),
    });
    const Shark = co.map({
      type: z.literal("shark"),
      ocean: Ocean,
    });

    const Animal = co.discriminatedUnion("type", [Dog, Shark]);
    const Species = co.list(Animal);

    let species: Loaded<typeof Species>;

    beforeEach(async () => {
      const group = Group.create();
      group.makePublic();

      species = Species.create(
        [
          {
            type: "dog",
            breed: {
              type: "collie",
            },
          },
          {
            type: "shark",
            ocean: {
              name: "atlantic",
            },
          },
        ],
        group,
      );
    });

    test("co.discriminatedUnion should load with deeply resolved mutually exclusive nested CoMaps", async () => {
      const loadedSpecies = await Species.load(species.$jazz.id, {
        resolve: {
          $each: {
            breed: true,
            ocean: true,
          },
        },
      });

      assertLoaded(loadedSpecies);

      // @ts-expect-error - type needs to be narrowed
      expect(loadedSpecies[0]?.breed.type).toEqual("collie");
      // @ts-expect-error - type needs to be narrowed
      expect(loadedSpecies[1]?.ocean.name).toEqual("atlantic");

      for (const animal of loadedSpecies) {
        if (animal.type === "dog") {
          expect(animal.breed.type).toBeDefined();
          // @ts-expect-error - no ocean property on Dog
          expect(animal.ocean).toBeUndefined();
        } else if (animal.type === "shark") {
          expect(animal.ocean.name).toBeDefined();
          // @ts-expect-error - no breed property on Shark
          expect(animal.breed).toBeUndefined();
        }
      }
    });

    test("co.discriminatedUnion should load with deeply resolved nested CoMaps with another account as owner", async () => {
      const alice = await createJazzTestAccount({
        creationProps: { name: "Alice" },
        isCurrentActiveAccount: false,
      });

      const loadedSpecies = await Species.load(species.$jazz.id, {
        loadAs: alice,
        resolve: {
          $each: {
            breed: true,
            ocean: true,
          },
        },
      });

      assertLoaded(loadedSpecies);

      for (const animal of loadedSpecies) {
        if (animal.type === "dog") {
          expect(animal.breed.type).toBeDefined();
          // @ts-expect-error - no ocean on Dog
          expect(animal.ocean).toBeUndefined();
        } else if (animal.type === "shark") {
          expect(animal.ocean.name).toBeDefined();
          // @ts-expect-error - no breed on Shark
          expect(animal.breed).toBeUndefined();
        }
      }
    });
  });

  test("cannot create a co.discriminatedUnion member if it has no matching discriminator value", async () => {
    const Dog = co.map({
      type: z.literal("dog"),
      name: z.string(),
    });
    const Cat = co.map({
      type: z.literal("cat"),
      name: z.string(),
    });
    const Parrot = co.map({
      type2: z.literal("parrot"),
      name: z.string(),
    });
    const Pet = co.discriminatedUnion("type", [Dog, Cat, Parrot]);
    const Pets = co.list(Pet);

    expect(() => Pets.create([{ type2: "parrot", name: "Polly" }])).toThrow(
      'Invalid discriminated union option at index "2"',
    );
  });

  test("can load a discriminated union CoList even if some elements are not accessible", async () => {
    const Dog = co.map({
      type: z.literal("dog"),
      name: z.string(),
    });
    const Cat = co.map({
      type: z.literal("cat"),
      name: z.string(),
    });
    const Pet = co.discriminatedUnion("type", [Dog, Cat]);
    const Pets = co.list(Pet);

    const publicGroup = Group.create().makePublic();
    const privateGroup = Group.create();
    const pets = Pets.create(
      [
        Dog.create({ type: "dog", name: "Rex" }, publicGroup),
        Cat.create({ type: "cat", name: "Whiskers" }, privateGroup),
      ],
      { owner: publicGroup },
    );

    const anotherAccount = await createJazzTestAccount();
    const loadedPets = await Pets.load(pets.$jazz.id, {
      resolve: { $each: { $onError: "catch" } },
      loadAs: anotherAccount,
    });

    assertLoaded(loadedPets);
    expect(loadedPets.length).toEqual(2);
    assert(loadedPets[0]);
    assertLoaded(loadedPets[0]);
    expect(loadedPets[0].type).toEqual("dog");
    expect(loadedPets[1]?.$jazz.loadingState).toEqual(
      CoValueLoadingState.UNAUTHORIZED,
    );
  });

  describe("Validation", () => {
    test("should throw when creating with invalid field types", () => {
      const Dog = co.map({
        type: z.literal("dog"),
        name: z.string(),
        age: z.number(),
      });
      const Cat = co.map({
        type: z.literal("cat"),
        name: z.string(),
        weight: z.number(),
      });
      const Pet = co.discriminatedUnion("type", [Dog, Cat]);
      const Person = co.map({
        pet: Pet,
      });

      expectValidationError(() =>
        Person.create({
          pet: {
            type: "dog",
            name: "Rex",
            // @ts-expect-error - age should be a number
            age: "5",
          },
        }),
      );
    });

    test("should throw when using already created CoValues", () => {
      const Dog = co.map({
        type: z.literal("dog"),
        name: z.string(),
        age: z.number(),
      });
      const Cat = co.map({
        type: z.literal("cat"),
        name: z.string(),
        weight: z.number(),
      });

      const Bird = co.map({
        type: z.literal("bird"),
        species: z.string(),
      });
      const Pet = co.discriminatedUnion("type", [Dog, Cat]);

      const Pets = co.list(Pet);

      const dog = Dog.create({
        type: "dog",
        name: "Rex",
        age: 5,
      });

      const cat = Cat.create({
        type: "cat",
        name: "Whiskers",
        weight: 10,
      });

      const bird = Bird.create({
        type: "bird",
        species: "Parrot",
      });

      expectValidationError(() =>
        Pets.create([
          dog,
          cat,
          // @ts-expect-error - bird is not a valid discriminator value
          bird,
        ]),
      );
    });

    test("should not throw when creating with invalid field types with loose validation", () => {
      const Dog = co.map({
        type: z.literal("dog"),
        name: z.string(),
        age: z.number(),
      });
      const Cat = co.map({
        type: z.literal("cat"),
        name: z.string(),
        weight: z.number(),
      });
      const Pet = co.discriminatedUnion("type", [Dog, Cat]);
      const Person = co.map({
        pet: Pet,
      });

      expect(() =>
        Person.create(
          {
            pet: {
              type: "dog",
              name: "Rex",
              // @ts-expect-error - age should be a number
              age: "5",
            },
          },
          { validation: "loose" },
        ),
      ).not.toThrow();
    });

    test("should throw when creating with wrong discriminator value", () => {
      const Dog = co.map({
        type: z.literal("dog"),
        name: z.string(),
      });
      const Cat = co.map({
        type: z.literal("cat"),
        name: z.string(),
      });
      const Pet = co.discriminatedUnion("type", [Dog, Cat]);
      const Person = co.map({
        pet: Pet,
      });

      expectValidationError(() =>
        Person.create({
          pet: {
            // @ts-expect-error - "bird" is not a valid discriminator value
            type: "bird",
            name: "Tweety",
          },
        }),
      );
    });

    test("should throw when mutating with invalid field types", () => {
      const Dog = co.map({
        type: z.literal("dog"),
        name: z.string(),
        age: z.number(),
      });
      const Cat = co.map({
        type: z.literal("cat"),
        name: z.string(),
        weight: z.number(),
      });
      const Pet = co.discriminatedUnion("type", [Dog, Cat]);
      const Person = co.map({
        pet: Pet,
      });

      const person = Person.create({
        pet: Dog.create({
          type: "dog",
          name: "Rex",
          age: 5,
        }),
      });

      if (person.pet.type === "dog") {
        const dog = person.pet;
        expectValidationError(() =>
          dog.$jazz.set(
            "age",
            // @ts-expect-error - age should be a number
            "6",
          ),
        );

        expect(dog.age).toEqual(5);
      }
    });

    test("should not throw when mutating with invalid field types with loose validation", () => {
      const Dog = co.map({
        type: z.literal("dog"),
        name: z.string(),
        age: z.number(),
      });
      const Cat = co.map({
        type: z.literal("cat"),
        name: z.string(),
        weight: z.number(),
      });
      const Pet = co.discriminatedUnion("type", [Dog, Cat]);
      const Person = co.map({
        pet: Pet,
      });

      const person = Person.create({
        pet: Dog.create({
          type: "dog",
          name: "Rex",
          age: 5,
        }),
      });

      if (person.pet.type === "dog") {
        const dog = person.pet;
        expect(() =>
          dog.$jazz.set(
            "age",
            // @ts-expect-error - age should be a number
            "6",
            { validation: "loose" },
          ),
        ).not.toThrow();

        expect(dog.age).toEqual("6");
      }
    });

    test("should throw when mutating to wrong union member type", () => {
      const Dog = co.map({
        type: z.literal("dog"),
        name: z.string(),
        age: z.number(),
      });
      const Cat = co.map({
        type: z.literal("cat"),
        name: z.string(),
        weight: z.number(),
      });
      const Pet = co.discriminatedUnion("type", [Dog, Cat]);
      const Person = co.map({
        pet: Pet,
      });

      const person = Person.create({
        pet: Dog.create({
          type: "dog",
          name: "Rex",
          age: 5,
        }),
      });

      if (person.pet.type === "dog") {
        const dog = person.pet;
        // Try to set a field that doesn't exist on Dog (weight is only on Cat)
        expectValidationError(() =>
          // @ts-expect-error - weight doesn't exist on Dog
          dog.$jazz.set("weight", 10),
        );
      }
    });

    test("should throw when mutating discriminator to invalid value", () => {
      const Dog = co.map({
        type: z.literal("dog"),
        name: z.string(),
        age: z.number(),
      });
      const Cat = co.map({
        type: z.literal("cat"),
        name: z.string(),
        weight: z.number(),
      });
      const Pet = co.discriminatedUnion("type", [Dog, Cat]);
      const Person = co.map({
        pet: Pet,
      });

      const person = Person.create({
        pet: Dog.create({
          type: "dog",
          name: "Rex",
          age: 5,
        }),
      });

      if (person.pet.type === "dog") {
        const dog = person.pet;
        expectValidationError(() =>
          dog.$jazz.set(
            "type",
            // @ts-expect-error - "bird" is not a valid discriminator value
            "bird",
          ),
        );
      }
    });

    test("loaded discriminated union keeps schema validation", async () => {
      const Dog = co.map({
        type: z.literal("dog"),
        name: z.string(),
        age: z.number(),
      });
      const Cat = co.map({
        type: z.literal("cat"),
        name: z.string(),
        weight: z.number(),
      });
      const Pet = co.discriminatedUnion("type", [Dog, Cat]);
      const Person = co.map({
        pet: Pet,
      });

      const person = Person.create({
        pet: Dog.create({
          type: "dog",
          name: "Rex",
          age: 5,
        }),
      });

      const loadedPerson = await Person.load(person.$jazz.id);

      assertLoaded(loadedPerson);
      assertLoaded(loadedPerson.pet);
      if (loadedPerson.pet.type === "dog") {
        const dog = loadedPerson.pet;
        expectValidationError(
          // @ts-expect-error - string is not a number
          () => dog.$jazz.set("age", "10"),
        );
      }
    });

    test("should throw when creating with missing required field", () => {
      const Dog = co.map({
        type: z.literal("dog"),
        name: z.string(),
        age: z.number(),
      });
      const Cat = co.map({
        type: z.literal("cat"),
        name: z.string(),
        weight: z.number(),
      });
      const Pet = co.discriminatedUnion("type", [Dog, Cat]);
      const Person = co.map({
        pet: Pet,
      });

      expectValidationError(() =>
        Person.create({
          pet: {
            type: "dog",
            name: "Rex",
            // age is required but missing
          } as any,
        }),
      );
    });

    test("should throw when mutating to set missing required field on different union member", () => {
      const Dog = co.map({
        type: z.literal("dog"),
        name: z.string(),
        age: z.number(),
      });
      const Cat = co.map({
        type: z.literal("cat"),
        name: z.string(),
        weight: z.number(),
      });
      const Pet = co.discriminatedUnion("type", [Dog, Cat]);
      const Person = co.map({
        pet: Pet,
      });

      const person = Person.create({
        pet: Dog.create({
          type: "dog",
          name: "Rex",
          age: 5,
        }),
      });

      if (person.pet.type === "dog") {
        const dog = person.pet;
        // Try to change type to cat but without providing required weight field
        expectValidationError(() => (dog.$jazz.set as any)("type", "cat"));
      }
    });
  });
});
