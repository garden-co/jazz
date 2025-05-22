import { assert, describe, expectTypeOf, test } from "vitest";
import { Group, co, z } from "../exports.js";
import { Account } from "../index.js";
import { CoListSchema, Loaded } from "../internal.js";

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

    test("has the _owner property", () => {
      const Person = co.map({
        name: z.string(),
      });

      const john = Person.create({ name: "John" }, Account.getMe());

      expectTypeOf(john._owner).toEqualTypeOf<Account | Group>();
    });

    test("CoMap with reference", () => {
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
      const Dog = co.map({
        name: z.string(),
        breed: co.map({ type: z.literal("labrador"), value: z.string() }),
      });
      type Dog = co.loaded<typeof Dog>;

      const Person = co.map({
        name: z.string(),
        age: z.number(),
        dog: Dog.optional(),
      });

      const dog = Dog.create({
        name: "Rex",
        breed: Dog.def.shape.breed.create({
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
        get child(): z.ZodOptional<typeof Recursive> {
          return z.optional(Recursive);
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
        // TODO: would be nice if this didn't need a type annotation
        get friend(): z.ZodOptional<typeof Person> {
          return z.optional(Person);
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

      john.dog = Dog.create({ name: "Fido" });
    });

    test("update a reference on a loaded value", () => {
      const Dog = co.map({
        name: z.string(),
        get siblings(): CoListSchema<typeof Dog> {
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

      john.dog = Dog.create({
        name: "Fido",
        siblings: co.list(Dog).create([]),
      });
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
      child: z.discriminatedUnion([ChildA, ChildB]),
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

    const loadedPerson = await Person.load(person.id, {
      resolve: {
        dog1: true,
      },
    });

    type ExpectedType = {
      name: string;
      age: number;
      dog1: Loaded<typeof Dog>;
      dog2: Loaded<typeof Dog> | null;
    } | null;

    function matches(value: ExpectedType) {
      return value;
    }

    matches(loadedPerson);

    assert(loadedPerson);
    expectTypeOf<typeof loadedPerson.dog1.name>().toEqualTypeOf<string>();
    expectTypeOf<typeof loadedPerson.dog2>().toEqualTypeOf<Loaded<
      typeof Dog
    > | null>();
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

    const loadedPerson = await Person.load(person.id, {
      resolve: {
        dog1: true,
        dog2: { $onError: null },
      },
    });

    type ExpectedType = {
      name: string;
      age: number;
      dog1: Loaded<typeof Dog>;
      dog2: Loaded<typeof Dog> | null;
    } | null;

    function matches(value: ExpectedType) {
      return value;
    }

    matches(loadedPerson);

    assert(loadedPerson);
    expectTypeOf<typeof loadedPerson.dog1.name>().toEqualTypeOf<string>();
    expectTypeOf<typeof loadedPerson.dog2>().toEqualTypeOf<
      | (Loaded<typeof Dog> & {
          $onError: never; // TODO: Clean the $onError from the type
        })
      | null
    >();
  });
});
