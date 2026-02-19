import { assert, describe, expectTypeOf, test } from "vitest";
import { ZodNumber, ZodOptional, ZodString } from "zod/v4";
import { Group, co, z } from "../exports.js";
import { Account } from "../index.js";
import { CoMap, Loaded, MaybeLoaded } from "../internal.js";
import { assertLoaded } from "./utils.js";

describe("CoMap", async () => {
  describe("init", () => {
    test("create a CoMap with basic property access", () => {
      const Person = co.map({
        color: z.string(),
        _height: z.number(),
        birthday: z.date(),
        name: z.string(),
        enum: z.enum(["a", "b", "c"]),
        enumMap: z.enum({ a: 1, b: 2, c: 3 }),
        optionalDate: z.date().optional(),
      });

      const birthday = new Date("1989-11-27");

      const john = Person.create({
        color: "red",
        _height: 10,
        birthday,
        enum: "a",
        enumMap: 1,
        name: "John",
      });

      type ExpectedType = {
        color: string;
        _height: number;
        birthday: Date;
        name: string;
        enum: "a" | "b" | "c";
        enumMap: 1 | 2 | 3;
        optionalDate: Date | undefined;
      };

      function matches(value: ExpectedType) {
        return value;
      }

      matches(john);
    });

    test("co.input returns the type for the init payload", () => {
      const Person = co.map({
        name: z.string(),
        age: z.number(),
        address: co.map({
          street: z.string(),
          city: z.string(),
        }),
      });

      const init = {} as co.input<typeof Person>;

      Person.create(init);
    });

    test("has the owner property", () => {
      const Person = co.map({
        name: z.string(),
      });

      const john = Person.create({ name: "John" }, Account.getMe());

      expectTypeOf(john.$jazz.owner).toEqualTypeOf<Group>();
    });

    test("create CoMap with reference using CoValue", () => {
      const Dog = co.map({
        name: z.string(),
        breed: z.string(),
      });

      const Person = co.map({
        name: z.string(),
        age: z.number(),
        dog: Dog,
      });

      const person = Person.create({
        name: "John",
        age: 20,
        dog: Dog.create({ name: "Rex", breed: "Labrador" }),
      });

      type ExpectedType = {
        name: string;
        age: number;
        dog: Loaded<typeof Dog>;
      };

      function matches(value: ExpectedType) {
        return value;
      }

      matches(person);
    });

    test("create CoMap with reference using JSON", () => {
      const Dog = co.map({
        name: z.string(),
        breed: z.string(),
      });
      const Person = co.map({
        dog1: Dog,
        dog2: Dog,
        get friend() {
          return Person.optional();
        },
      });

      // @ts-expect-error - Object literal may only specify known properties
      const person = Person.create({
        // @ts-expect-error - breed is missing
        dog1: { name: "Rex", items },
        dog2: { name: "Fido", breed: "Labrador", extra: "extra" },
        friend: {
          dog1: {
            name: "Rex",
            breed: "Labrador",
          },
          dog2: { name: "Fido", breed: "Labrador" },
        },
      });

      type ExpectedType = {
        dog1: Loaded<typeof Dog>;
        dog2: Loaded<typeof Dog>;
        friend: Loaded<typeof Person> | undefined;
      };

      function matches(value: ExpectedType) {
        return value;
      }

      matches(person);
    });

    test("CoMap with optional reference", () => {
      const Dog = co.map({
        name: z.string(),
        breed: z.string(),
      });

      const Person = co.map({
        name: z.string(),
        age: z.number(),
        dog: Dog.optional(),
      });

      const person = Person.create({
        name: "John",
        age: 20,
        dog: Dog.create({ name: "Rex", breed: "Labrador" }),
      });

      type ExpectedType = {
        name: string;
        age: number;
        dog: Loaded<typeof Dog> | undefined;
      };

      function matches(value: ExpectedType) {
        return value;
      }

      matches(person);
    });

    test("CoMap create with partially loaded, reference and optional", () => {
      const Breed = co.map({ type: z.literal("labrador"), value: z.string() });
      const Dog = co.map({
        name: z.string(),
        breed: Breed,
      });
      type Dog = co.loaded<typeof Dog>;

      const Person = co.map({
        name: z.string(),
        age: z.number(),
        dog: Dog.optional(),
      });

      const dog = Dog.create({
        name: "Rex",
        breed: Breed.create({
          type: "labrador",
          value: "Labrador",
        }),
      }) as Dog;

      const person = Person.create({
        name: "John",
        age: 20,
        dog,
      });

      type ExpectedType = {
        name: string;
        age: number;
        dog: Loaded<typeof Dog> | undefined;
      };

      function matches(value: ExpectedType) {
        return value;
      }

      matches(person);
    });

    test("Comap with recursive optional reference", () => {
      const Recursive = co.map({
        get child() {
          return Recursive.optional();
        },
      });

      const child: Loaded<typeof Recursive> = Recursive.create({});
      const parent = Recursive.create({
        child: child,
      });

      type ExpectedType = {
        child: Loaded<typeof Recursive> | undefined;
      };

      function matches(value: ExpectedType) {
        return value;
      }

      matches(parent);
    });

    test("CoMap with self reference", () => {
      const Person = co.map({
        name: z.string(),
        age: z.number(),
        get friend() {
          return co.optional(Person);
        },
      });

      const person = Person.create({
        name: "John",
        age: 20,
        friend: Person.create({ name: "Jane", age: 21 }),
      });

      type ExpectedType = {
        name: string;
        age: number;
        friend: Loaded<typeof Person> | undefined;
      };

      function matches(value: ExpectedType) {
        return value;
      }

      matches(person);
    });

    test("should disallow extra properties", () => {
      const Person = co.map({
        name: z.string(),
        age: z.number(),
      });

      // @ts-expect-error - x is not a valid property
      Person.create({ name: "John", age: 30, xtra: 1 });
    });

    test("create options", () => {
      const Person = co.map({
        name: z.string(),
      });

      Person.create({ name: "John" });
      Person.create({ name: "John" }, Group.create());
      Person.create({ name: "John" }, {});
      Person.create({ name: "John" }, { owner: Group.create() });
      Person.create(
        { name: "John" },
        { owner: Group.create(), unique: "test" },
      );

      // @ts-expect-error - owner is required if unique is provided
      Person.create({ name: "John" }, { unique: "test" });

      // this is deprecated but valid
      Person.create({ name: "John" }, Account.getMe());
      Person.create(
        { name: "John" },
        { owner: Account.getMe(), unique: "test" },
      );

      Person.create({ name: "John" }, { validation: "loose" });
      Person.create(
        { name: "John" },
        { owner: Group.create(), validation: "loose" },
      );
      // @ts-expect-error - owner is required if unique is provided
      Person.create({ name: "John" }, { unique: "test", validation: "loose" });
    });
  });

  describe("Mutation", () => {
    test("update a reference", () => {
      const Dog = co.map({
        name: z.string(),
      });

      const Person = co.map({
        name: z.string(),
        age: z.number(),
        dog: Dog,
      });

      const john = Person.create({
        name: "John",
        age: 20,
        dog: Dog.create({ name: "Rex" }),
      });

      john.$jazz.set("dog", Dog.create({ name: "Fido" }));
    });

    test("cannot update a non-existing key", () => {
      const Person = co.map({
        name: z.string(),
      });

      const john = Person.create({ name: "John" });

      // @ts-expect-error - Argument of type '"non-existing-key"' is not assignable to parameter of type '"name"'
      john.$jazz.set("non-existing-key", "Jane");
    });

    test("cannot set a value with an incorrect type", () => {
      const Person = co.map({
        name: z.string(),
      });

      const john = Person.create({ name: "John" });

      // @ts-expect-error - Argument of type 'number' is not assignable to parameter of type 'string'
      john.$jazz.set("name", 12);
    });

    test("update a reference on a loaded value", () => {
      const Dog = co.map({
        name: z.string(),
        get siblings() {
          return co.list(Dog);
        },
      });

      const Person = co.map({
        name: z.string(),
        age: z.number(),
        dog: Dog,
      });

      const john = Person.create({
        name: "John",
        age: 20,
        dog: Dog.create({ name: "Rex", siblings: co.list(Dog).create([]) }),
      }) as Loaded<typeof Person, { dog: { siblings: true } }>;

      john.$jazz.set(
        "dog",
        Dog.create({
          name: "Fido",
          siblings: co.list(Dog).create([]),
        }),
      );
    });
  });

  test("Enum of maps", () => {
    const ChildA = co.map({
      type: z.literal("a"),
      value: z.number(),
    });

    const ChildB = co.map({
      type: z.literal("b"),
      value: z.string(),
    });

    const MapWithEnumOfMaps = co.map({
      name: z.string(),
      child: co.discriminatedUnion("type", [ChildA, ChildB]),
    });

    const mapWithEnum = MapWithEnumOfMaps.create({
      name: "enum",
      child: ChildA.create({
        type: "a",
        value: 5,
      }),
    });

    type ExpectedType = {
      name: string;
      child: Loaded<typeof ChildA> | Loaded<typeof ChildB>;
    };

    function matches(value: ExpectedType) {
      return value;
    }

    matches(mapWithEnum);

    function matchesNarrowed(value: Loaded<typeof ChildA>) {
      return value;
    }

    if (mapWithEnum.child.type === "a") {
      matchesNarrowed(mapWithEnum.child);
    }
  });

  test("CoMap.pick()", () => {
    const Person = co.map({
      name: z.string(),
      age: z.number(),
      dog: co.map({
        name: z.string(),
        breed: z.string(),
      }),
    });

    const PersonWithoutDog = Person.pick({
      name: true,
      age: true,
    });

    type ExpectedType = co.Map<{
      name: ZodString;
      age: ZodNumber;
    }>;

    function matches(value: ExpectedType) {
      return value;
    }

    matches(PersonWithoutDog);
  });

  test("CoMap.pick() with a recursive reference", () => {
    const Person = co.map({
      name: z.string(),
      age: z.number(),
      dog: co.map({
        name: z.string(),
        breed: z.string(),
      }),
      get friend() {
        return Person.pick({
          name: true,
          age: true,
        }).optional();
      },
    });

    type ExpectedType = co.Map<{
      name: ZodString;
      age: ZodNumber;
      dog: co.Map<{
        name: ZodString;
        breed: ZodString;
      }>;
      friend: co.Optional<
        co.Map<{
          name: ZodString;
          age: ZodNumber;
        }>
      >;
    }>;

    function matches(value: ExpectedType) {
      return value;
    }

    matches(Person);
  });

  test("CoMap.partial()", () => {
    const Person = co.map({
      name: z.string(),
      age: z.number(),
      dog: co.map({
        name: z.string(),
        breed: z.string(),
      }),
    });

    const PersonPartial = Person.partial();

    type ExpectedType = co.Map<{
      name: ZodOptional<ZodString>;
      age: ZodOptional<ZodNumber>;
      dog: co.Optional<
        co.Map<{
          name: ZodString;
          breed: ZodString;
        }>
      >;
    }>;

    function matches(value: ExpectedType) {
      return value;
    }

    matches(PersonPartial);
  });

  test("CoMap.partial() with a recursive reference", () => {
    const Person = co.map({
      get draft() {
        return Person.partial()
          .pick({
            name: true,
            age: true,
            dog: true,
          })
          .optional();
      },
      name: z.string(),
      age: z.number(),
      dog: co.map({
        name: z.string(),
        breed: z.string(),
      }),
    });

    type ExpectedType = co.Map<{
      draft: co.Optional<
        co.Map<{
          name: ZodOptional<ZodString>;
          age: ZodOptional<ZodNumber>;
          dog: co.Optional<
            co.Map<{
              name: ZodString;
              breed: ZodString;
            }>
          >;
        }>
      >;
      name: ZodString;
      age: ZodNumber;
      dog: co.Map<{
        name: ZodString;
        breed: ZodString;
      }>;
    }>;

    function matches(value: ExpectedType) {
      return value;
    }

    matches(Person);
  });
});

describe("CoMap resolution", async () => {
  test("partial loading a map with deep resolve", async () => {
    const Dog = co.map({
      name: z.string(),
      breed: z.string(),
    });

    const Person = co.map({
      name: z.string(),
      age: z.number(),
      dog1: Dog,
      dog2: Dog,
    });

    const person = Person.create({
      name: "John",
      age: 20,
      dog1: Dog.create({ name: "Rex", breed: "Labrador" }),
      dog2: Dog.create({ name: "Fido", breed: "Poodle" }),
    });

    const loadedPerson = await Person.load(person.$jazz.id, {
      resolve: {
        dog1: true,
      },
    });

    type ExpectedType = MaybeLoaded<{
      name: string;
      age: number;
      dog1: Loaded<typeof Dog>;
      dog2: MaybeLoaded<Loaded<typeof Dog>>;
    }>;

    function matches(value: ExpectedType) {
      return value;
    }

    matches(loadedPerson);

    assertLoaded(loadedPerson);
    expectTypeOf<typeof loadedPerson.dog1.name>().toEqualTypeOf<string>();
    expectTypeOf<typeof loadedPerson.dog2>().branded.toEqualTypeOf<
      MaybeLoaded<Loaded<typeof Dog>>
    >();
  });

  test("partial loading a map with string resolve", async () => {
    const Dog = co.map({
      name: z.string(),
      breed: z.string(),
    });

    const Person = co.map({
      name: z.string(),
      age: z.number(),
      dog1: Dog,
      dog2: Dog,
    });

    const person = Person.create({
      name: "John",
      age: 20,
      dog1: Dog.create({ name: "Rex", breed: "Labrador" }),
      dog2: Dog.create({ name: "Fido", breed: "Poodle" }),
    });

    const userId: string = "dog1";

    const loadedPerson = await Person.load(person.$jazz.id, {
      resolve: {
        [userId]: true,
      },
    });

    type ExpectedType = MaybeLoaded<{
      name: string;
      age: number;
      dog1: MaybeLoaded<Loaded<typeof Dog>>;
      dog2: MaybeLoaded<Loaded<typeof Dog>>;
    }>;

    function matches(value: ExpectedType) {
      return value;
    }

    matches(loadedPerson);

    assertLoaded(loadedPerson);
    expectTypeOf<typeof loadedPerson.dog1>().branded.toEqualTypeOf<
      MaybeLoaded<Loaded<typeof Dog>>
    >();
    expectTypeOf<typeof loadedPerson.dog2>().branded.toEqualTypeOf<
      MaybeLoaded<Loaded<typeof Dog>>
    >();
  });

  test("loading a map with deep resolve and $onError", async () => {
    const Dog = co.map({
      name: z.string(),
      breed: z.string(),
    });

    const Person = co.map({
      name: z.string(),
      age: z.number(),
      dog1: Dog,
      dog2: Dog,
    });

    const person = Person.create({
      name: "John",
      age: 20,
      dog1: Dog.create({ name: "Rex", breed: "Labrador" }),
      dog2: Dog.create({ name: "Fido", breed: "Poodle" }),
    });

    const loadedPerson = await Person.load(person.$jazz.id, {
      resolve: {
        dog1: true,
        dog2: { $onError: "catch" },
      },
    });

    type ExpectedType = MaybeLoaded<{
      name: string;
      age: number;
      dog1: Loaded<typeof Dog>;
      dog2: MaybeLoaded<Loaded<typeof Dog>>;
    }>;

    function matches(value: ExpectedType) {
      return value;
    }

    matches(loadedPerson);

    assertLoaded(loadedPerson);
    expectTypeOf<typeof loadedPerson.dog1.name>().toEqualTypeOf<string>();
    expectTypeOf<typeof loadedPerson.dog2>().branded.toEqualTypeOf<
      MaybeLoaded<Loaded<typeof Dog>>
    >();
  });

  test("loading a map with a nullable field", async () => {
    const Dog = co.map({
      name: z.string(),
      breed: z.string(),
    });
    const Person = co.map({
      name: z.string(),
      age: z.number().nullable(),
      dog: Dog,
    });

    const person = Person.create({
      name: "John",
      age: 20,
      dog: Dog.create({ name: "Rex", breed: "Labrador" }),
    });

    const loadedPerson = await Person.load(person.$jazz.id);

    assertLoaded(loadedPerson);
    expectTypeOf(loadedPerson).branded.toEqualTypeOf<
      {
        readonly name: string;
        readonly age: number | null;
        readonly dog: MaybeLoaded<Loaded<typeof Dog>>;
      } & CoMap
    >();
  });
});
