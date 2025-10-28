import { describe, expectTypeOf, it } from "vitest";
import { CoMap, coField } from "../index.js";

describe("coField.json TypeScript validation", () => {
  it("should accept serializable types", async () => {
    type ValidType = { str: string; num: number; bool: boolean };

    class ValidPrimitiveMap extends CoMap {
      data = coField.json<ValidType>();
    }

    expectTypeOf(ValidPrimitiveMap.create<ValidPrimitiveMap>)
      .parameter(0)
      .toEqualTypeOf<{
        data: ValidType;
      }>();
  });

  it("should accept nested serializable types", async () => {
    type NestedType = {
      outer: {
        inner: {
          value: string;
        };
      };
    };

    class ValidNestedMap extends CoMap {
      data = coField.json<NestedType>();
    }

    expectTypeOf(ValidNestedMap.create<ValidNestedMap>)
      .parameter(0)
      .toEqualTypeOf<{
        data: NestedType;
      }>();
  });

  it("should accept types with optional attributes", async () => {
    type TypeWithOptional = {
      value: string;
      optional?: string | null;
    };

    class ValidMap extends CoMap {
      data = coField.json<TypeWithOptional>();
    }

    expectTypeOf(ValidMap.create<ValidMap>)
      .parameter(0)
      .toEqualTypeOf<{
        data: TypeWithOptional;
      }>();
  });

  it("should accept nested serializable interfaces", async () => {
    interface InnerInterface {
      value: string;
    }

    interface NestedInterface {
      outer: {
        inner: InnerInterface;
      };
    }

    class ValidNestedMap extends CoMap {
      data = coField.json<NestedInterface>();
    }

    expectTypeOf(ValidNestedMap.create<ValidNestedMap>)
      .parameter(0)
      .toEqualTypeOf<{
        data: NestedInterface;
      }>();
  });

  it("should accept arrays of serializable types", async () => {
    interface ArrayInterface {
      numbers: number[];
      objects: { id: number; name: string }[];
    }

    class ValidArrayMap extends CoMap {
      data = coField.json<ArrayInterface>();
    }

    expectTypeOf(ValidArrayMap.create<ValidArrayMap>)
      .parameter(0)
      .toEqualTypeOf<{
        data: ArrayInterface;
      }>();
  });

  it("should flag interfaces with functions as invalid", async () => {
    interface InvalidInterface {
      func: () => void;
    }

    class InvalidFunctionMap extends CoMap {
      // @ts-expect-error Should not be considered valid
      data = coField.json<InvalidInterface>();
    }

    expectTypeOf(InvalidFunctionMap.create<InvalidFunctionMap>)
      .parameter(0)
      .toEqualTypeOf<{
        data: InvalidInterface;
      }>();
  });

  it("should flag types with functions as invalid", async () => {
    type InvalidType = { func: () => void };

    class InvalidFunctionMap extends CoMap {
      // @ts-expect-error Should not be considered valid
      data = coField.json<InvalidType>();
    }

    expectTypeOf(InvalidFunctionMap.create<InvalidFunctionMap>)
      .parameter(0)
      .toEqualTypeOf<{
        data: InvalidType;
      }>();
  });

  it("should flag types with non-serializable constructors as invalid", async () => {
    type InvalidType = { date: Date; regexp: RegExp; symbol: symbol };

    class InvalidFunctionMap extends CoMap {
      // @ts-expect-error Should not be considered valid
      data = coField.json<InvalidType>();
    }

    expectTypeOf(InvalidFunctionMap.create<InvalidFunctionMap>)
      .parameter(0)
      .toEqualTypeOf<{
        data: InvalidType;
      }>();
  });

  it("should flag types with symbol keys as invalid", async () => {
    type InvalidType = { [key: symbol]: string };

    class InvalidFunctionMap extends CoMap {
      // @ts-expect-error Should not be considered valid
      data = coField.json<InvalidType>();
    }
  });

  it("should apply the same validation to optional json", async () => {
    type ValidType = {
      value: string;
    };

    type InvalidType = {
      value: () => string;
    };

    class MapWithOptionalJSON extends CoMap {
      data = coField.optional.json<ValidType>();
      // @ts-expect-error Should not be considered valid
      data2 = coField.optional.json<InvalidType>();
    }

    expectTypeOf(MapWithOptionalJSON.create<MapWithOptionalJSON>)
      .parameter(0)
      .toEqualTypeOf<{
        data?: ValidType | null;
        data2?: InvalidType | null;
      }>();
  });

  /* Special case from reported issue:
   ** See: https://github.com/garden-co/jazz/issues/1496
   */
  it("should apply the same validation to optional json [JAZZ-1496]", async () => {
    interface ValidInterface0 {
      value: string;
    }
    interface ValidInterface1 {
      value: string | undefined;
    }
    interface InterfaceWithOptionalTypes {
      requiredValue: string;
      value?: string;
    }

    class MapWithOptionalJSON extends CoMap {
      data1 = coField.optional.json<ValidInterface0>();
      data2 = coField.optional.json<ValidInterface1>();
      data3 = coField.optional.json<InterfaceWithOptionalTypes>();
    }

    expectTypeOf(MapWithOptionalJSON.create<MapWithOptionalJSON>)
      .parameter(0)
      .toEqualTypeOf<{
        data1?: ValidInterface0 | null;
        data2?: ValidInterface1 | null;
        data3?: InterfaceWithOptionalTypes | null;
      }>();
  });

  it("should not accept functions", async () => {
    class InvalidFunctionMap extends CoMap {
      // @ts-expect-error Should not be considered valid
      data = coField.json<() => void>();
    }
  });

  it("should not accept functions in nested properties", async () => {
    class InvalidFunctionMap extends CoMap {
      // @ts-expect-error Should not be considered valid
      data = coField.json<{ func: () => void }>();
    }
  });

  it("should not accept RegExp", async () => {
    class InvalidFunctionMap extends CoMap {
      // @ts-expect-error Should not be considered valid
      data = coField.json<RegExp>();
    }

    expectTypeOf(InvalidFunctionMap.create<InvalidFunctionMap>)
      .parameter(0)
      .toEqualTypeOf<{
        data: RegExp;
      }>();
  });

  it("should accept strings and numbers", async () => {
    class InvalidFunctionMap extends CoMap {
      str = coField.json<string>();
      num = coField.json<number>();
    }

    expectTypeOf(InvalidFunctionMap.create<InvalidFunctionMap>)
      .parameter(0)
      .toEqualTypeOf<{
        str: string;
        num: number;
      }>();
  });
});
