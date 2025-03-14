import { beforeEach, describe, expect, expectTypeOf, it } from "vitest";
import { createJazzTestAccount } from "../testing.js";
import { CoMapJazzApi } from "./coMap/instance.js";
import {
  CoMapInit,
  CoMapInitToRelationsToResolve,
  CoMapSchema,
} from "./coMap/schema.js";
import { CoMap, Loaded, ResolveQuery, co, z } from "./schema.js";

beforeEach(async () => {
  await createJazzTestAccount({
    isCurrentActiveAccount: true,
  });
});

describe("CoMap - with zod based schema", () => {
  describe("ResolveQuery", () => {
    it("should properly parse a schema without relations", () => {
      const Person = co.map({
        name: z.string(),
        age: z.number(),
      });

      type Result = ResolveQuery<typeof Person>;

      expectTypeOf<Result>().toEqualTypeOf<true>();
    });

    it("should properly parse a schema with a relation", () => {
      const Person = co.map({
        name: z.string(),
        age: z.number(),
        address: co.map({
          street: z.string(),
        }),
      });

      type Result = ResolveQuery<typeof Person>;

      expectTypeOf<Result>().toEqualTypeOf<
        | true
        | {
            address?: true;
          }
      >();
    });

    it("should pick up relations from the catchall", () => {
      const Person = co
        .map({
          name: z.string(),
          age: z.number(),
        })
        .catchall(
          co.map({
            street: z.string(),
          }),
        );

      type Result = ResolveQuery<typeof Person>;

      expectTypeOf<Result>().toEqualTypeOf<
        | true
        | {
            [x: string]: true | undefined;
            $each?: true;
          }
      >();
    });

    it("should properly parse a schema with a self reference", () => {
      const Person = co.map({
        name: z.string(),
        age: z.number(),
        friend: co.self(),
      });

      type Result = ResolveQuery<typeof Person>;

      expectTypeOf<Result>().toMatchTypeOf<
        | true
        | {
            friend?:
              | true
              | {
                  friend?:
                    | true
                    | {
                        friend?:
                          | true
                          | {
                              friend?:
                                | true
                                | {
                                    friend?: true;
                                  };
                            };
                      };
                };
          }
      >();
    });

    it("should return the same result when providing a SchemaDefinition", () => {
      const Person = co.map({
        name: z.string(),
        age: z.number(),
        address: co.map({
          street: z.string(),
        }),
      });

      type PersonSchema = typeof Person;
      type PersonSchemaDefinition = CoMapSchema<
        PersonSchema["shape"],
        PersonSchema["record"],
        PersonSchema["isOptional"]
      >;
      type Result = ResolveQuery<PersonSchemaDefinition>;

      expectTypeOf<Result>().toEqualTypeOf<
        | true
        | {
            address?: true;
          }
      >();
    });
  });

  describe("Loaded", () => {
    it("should properly parse a schema without relations", () => {
      const Person = co.map({
        name: z.string(),
        age: z.number(),
      });

      type Result = Loaded<typeof Person, true>;

      expectTypeOf<Result>().toMatchTypeOf<
        {
          name: string;
          age: number;
        } & CoMap<typeof Person, true>
      >();
    });

    it("should properly parse a schema with a relation", () => {
      const Person = co.map({
        name: z.string(),
        age: z.number(),
        address: co.map({
          street: z.string(),
        }),
      });

      type Result = Loaded<typeof Person, true>;

      expectTypeOf<Result>().toMatchTypeOf<
        {
          name: string;
          age: number;
          address: null;
        } & CoMap<typeof Person, true>
      >();
    });

    it("should properly parse a schema with a relation {address: true}", () => {
      const Person = co.map({
        name: z.string(),
        age: z.number(),
        address: co.map({
          street: z.string(),
        }),
      });

      type Result = Loaded<typeof Person, { address: true }>;

      expectTypeOf<Result>().toMatchTypeOf<
        {
          name: string;
          age: number;
          address: {
            street: string;
          } & CoMap<typeof Person.shape.address, true>;
        } & CoMap<typeof Person, { address: true }>
      >();
    });

    it("should properly parse a schema with an optional relation {address: true}", () => {
      const Person = co.map({
        name: z.string(),
        age: z.number(),
        address: co
          .map({
            street: z.string(),
          })
          .optional(),
      });

      type Result = Loaded<typeof Person, { address: true }>;

      expectTypeOf<Result>().toMatchTypeOf<
        {
          name: string;
          age: number;
          address:
            | ({
                street: string;
              } & CoMap<typeof Person.shape.address, true>)
            | null
            | undefined;
        } & CoMap<typeof Person, { address: true }>
      >();
    });

    it("should pick up properties from the catchall", () => {
      const Person = co
        .map({
          name: z.string(),
        })
        .catchall(z.string());

      type Result = Loaded<typeof Person, { address: true }>;

      expectTypeOf<Result>().toMatchTypeOf<{
        name: string;
        [x: string]: string;
      }>();
    });

    it("should pick up relations from the catchall", () => {
      const Person = co
        .map({
          name: z.string(),
          age: z.number(),
        })
        .catchall(
          co.map({
            street: z.string(),
          }),
        );

      type Result = Loaded<typeof Person, { address: true }>;

      expectTypeOf<Result>().toMatchTypeOf<{
        name: string;
        age: number;
        address:
          | {
              street: string;
              $jazz: any;
            }
          | null
          | undefined;
        $jazz: any;
      }>();
    });

    it("should load nested properties on co.record when using $each", () => {
      const Friends = co.record(
        z.string(),
        co.map({
          name: z.string(),
        }),
      );

      type Result = Loaded<typeof Friends, { $each: true }>;

      expectTypeOf<Result>().toMatchTypeOf<{
        [x: string]:
          | {
              name: string;
              $jazz: any;
            }
          | null
          | undefined;
        $jazz: any;
      }>();
    });

    it("should properly parse a schema with a self reference", () => {
      const Person = co.map({
        name: z.string(),
        age: z.number(),
        friend: co.self(),
      });

      type Result = Loaded<typeof Person, true>;

      expectTypeOf<Result>().toMatchTypeOf<
        {
          name: string;
          age: number;
          friend: null;
        } & CoMap<typeof Person, true>
      >();
    });

    it("should properly parse a schema with a self reference {friend: true}", () => {
      const Person = co.map({
        name: z.string(),
        age: z.number(),
        friend: co.self(),
      });

      type Result = Loaded<typeof Person, { friend: true }>;

      expectTypeOf<Result>().toMatchTypeOf<
        {
          name: string;
          age: number;
          friend:
            | ({
                name: string;
                age: number;
                friend: null;
              } & CoMap<typeof Person, true>)
            | null
            | undefined;
        } & CoMap<typeof Person, { friend: true }>
      >();
    });

    it("should return the same result when providing a SchemaDefinition", () => {
      const Person = co.map({
        name: z.string(),
        age: z.number(),
        address: co.map({
          street: z.string(),
        }),
      });

      type PersonSchema = typeof Person;
      type PersonSchemaDefinition = CoMapSchema<
        PersonSchema["shape"],
        PersonSchema["record"],
        PersonSchema["isOptional"]
      >;
      type Result = Loaded<PersonSchemaDefinition, true>;

      expectTypeOf<Result>().toMatchTypeOf<{
        name: string;
        age: number;
        address: null;
        $jazz: CoMapJazzApi<PersonSchemaDefinition, true>;
      }>();
    });

    it("should return the same result when providing a SchemaDefinition {address: true}", () => {
      const Person = co.map({
        name: z.string(),
        age: z.number(),
        address: co.map({
          street: z.string(),
        }),
      });

      type PersonSchema = typeof Person;
      type PersonSchemaDefinition = CoMapSchema<
        PersonSchema["shape"],
        PersonSchema["record"],
        PersonSchema["isOptional"]
      >;
      type Result = Loaded<PersonSchemaDefinition, { address: true }>;

      expectTypeOf<Result>().toMatchTypeOf<{
        name: string;
        age: number;
        address: {
          street: string;
          $jazz: any;
        };
        $jazz: any;
      }>();
    });
  });

  describe("CoMapInit", () => {
    it("should properly parse a schema without relations", () => {
      const Person = co.map({
        name: z.string(),
        age: z.number().optional(),
      });

      type Result = CoMapInit<typeof Person>;

      expectTypeOf<Result>().toMatchTypeOf<{
        name: string;
        age?: number;
      }>();
    });

    it("should properly parse a schema with a relation", () => {
      const Person = co.map({
        name: z.string(),
        age: z.number(),
        address: co.map({
          street: z.string(),
        }),
      });

      type Result = CoMapInit<typeof Person>;

      expectTypeOf<Result>().toMatchTypeOf<{
        name: string;
        age: number;
        address:
          | {
              street: string;
            }
          | ({
              street: string;
            } & CoMap<typeof Person.shape.address, true>);
      }>();
    });

    it("should properly parse a schema with an optional relation", () => {
      const Person = co.map({
        name: z.string(),
        age: z.number(),
        address: co
          .map({
            street: z.string(),
          })
          .optional(),
      });

      type Result = CoMapInit<typeof Person>;

      expectTypeOf<Result>().toMatchTypeOf<{
        name: string;
        age: number;
        address?:
          | {
              street: string;
            }
          | ({
              street: string;
            } & CoMap<typeof Person.shape.address, true>);
      }>();
    });

    it("should properly parse a schema with a catchall", () => {
      const Person = co
        .map({
          name: z.string(),
          age: z.number(),
        })
        .catchall(
          co.map({
            street: z.string(),
          }),
        );

      type Result = CoMapInit<typeof Person>;

      expectTypeOf<Result>().toMatchTypeOf<
        {
          name: string;
          age: number;
        } & Record<
          string,
          | {
              street: string;
            }
          | {
              readonly street: string;
              $jazz: any;
            }
        >
      >;
    });

    it("should properly parse a schema with a self reference", () => {
      const Person = co.map({
        name: z.string(),
        age: z.number(),
        friend: co.self(),
      });

      // This is just for debug, writing an expectation is not possible
      type Result = CoMapInit<typeof Person>;
    });

    it("should return the same result when providing a SchemaDefinition", () => {
      const Person = co.map({
        name: z.string(),
        age: z.number(),
        address: co.map({
          street: z.string(),
        }),
      });

      type PersonSchema = typeof Person;
      type PersonSchemaDefinition = CoMapSchema<
        PersonSchema["shape"],
        PersonSchema["record"],
        PersonSchema["isOptional"]
      >;
      type Result = CoMapInit<PersonSchemaDefinition>;

      expectTypeOf<Result>().toMatchTypeOf<{
        name: string;
        age: number;
        address?:
          | {
              street: string;
            }
          | ({
              street: string;
            } & CoMap<typeof Person.shape.address, true>);
      }>();
    });
  });

  describe("CoMapInitToRelationsToResolve", () => {
    it("should properly parse a schema without relations", () => {
      const Person = co.map({
        name: z.string(),
        age: z.number(),
      });

      type Result = CoMapInitToRelationsToResolve<
        typeof Person,
        {
          name: string;
          age: number;
        }
      >;

      expectTypeOf<Result>().toEqualTypeOf<true>();
    });

    it("should properly parse a schema with a relation", () => {
      const Person = co.map({
        name: z.string(),
        age: z.number(),
        address: co.map({
          street: z.string(),
        }),
      });

      type Result = CoMapInitToRelationsToResolve<
        typeof Person,
        {
          name: string;
          age: number;
          address: {
            street: string;
          };
        }
      >;

      expectTypeOf<Result>().toEqualTypeOf<{
        address: true;
      }>();
    });

    it("should properly parse a schema with a catchal relation", () => {
      const Person = co
        .map({
          name: z.string(),
          age: z.number(),
        })
        .catchall(
          co.map({
            street: z.string(),
          }),
        );

      type Result = CoMapInitToRelationsToResolve<
        typeof Person,
        {
          address: {
            street: string;
          };
        }
      >;

      expectTypeOf<Result>().toMatchTypeOf<{
        address: true;
      }>;
    });

    it("should properly parse a schema with a self reference", () => {
      const Person = co.map({
        name: z.string(),
        age: z.number(),
        friend: co.self(),
      });

      type Result = CoMapInitToRelationsToResolve<
        typeof Person,
        {
          name: string;
          age: number;
          friend: {
            name: string;
            age: number;
          };
        }
      >;

      expectTypeOf<Result>().toEqualTypeOf<{
        friend: true;
      }>();
    });

    it("should return the same result when providing a SchemaDefinition", () => {
      const Person = co.map({
        name: z.string(),
        age: z.number(),
        address: co.map({
          street: z.string(),
        }),
      });

      type PersonSchema = typeof Person;
      type PersonSchemaDefinition = CoMapSchema<
        PersonSchema["shape"],
        PersonSchema["record"],
        PersonSchema["isOptional"]
      >;
      type Result = ResolveQuery<PersonSchemaDefinition>;

      expectTypeOf<Result>().toEqualTypeOf<
        | true
        | {
            address?: true;
          }
      >();
    });
  });
});
