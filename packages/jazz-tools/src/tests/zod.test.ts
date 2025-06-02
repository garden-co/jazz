import { describe, expect, it, vi } from "vitest";
import { z } from "../exports.js";
import { co } from "../internal.js";
import { createJazzTestAccount } from "../testing.js";

describe("co.map and Zod schema compatibility", () => {
  // Helper function to create a test account
  describe("Primitive types", () => {
    it("should handle string fields", async () => {
      const schema = co.map({
        name: z.string(),
      });
      const account = await createJazzTestAccount();
      const map = schema.create({ name: "Test" }, account);
      expect(map.name).toBe("Test");
    });

    it("should handle number fields", async () => {
      const schema = co.map({
        age: z.number(),
      });
      const account = await createJazzTestAccount();
      const map = schema.create({ age: 42 }, account);
      expect(map.age).toBe(42);
    });

    it("should handle boolean fields", async () => {
      const schema = co.map({
        isActive: z.boolean(),
      });
      const account = await createJazzTestAccount();
      const map = schema.create({ isActive: true }, account);
      expect(map.isActive).toBe(true);
    });

    it("should handle date fields", async () => {
      const schema = co.map({
        createdAt: z.date(),
      });
      const account = await createJazzTestAccount();
      const date = new Date();
      const map = schema.create({ createdAt: date }, account);
      expect(map.createdAt).toEqual(date);
    });

    it("should handle optional date fields", async () => {
      const schema = co.map({
        createdAt: z.date().optional(),
      });
      const account = await createJazzTestAccount();
      const map = schema.create({ createdAt: undefined }, account);
      expect(map.createdAt).toEqual(undefined);
    });

    it("should handle literal fields", async () => {
      const schema = co.map({
        status: z.literal("active"),
      });
      const account = await createJazzTestAccount();
      const map = schema.create({ status: "active" }, account);
      expect(map.status).toBe("active");
    });
  });

  describe("String validation types", () => {
    it("should handle email fields", async () => {
      const schema = co.map({
        email: z.email(),
      });
      const account = await createJazzTestAccount();
      const map = schema.create({ email: "test@example.com" }, account);
      expect(map.email).toBe("test@example.com");
    });

    it("should handle uuid fields", async () => {
      const schema = co.map({
        uid: z.uuid(),
      });
      const account = await createJazzTestAccount();
      const map = schema.create(
        { uid: "123e4567-e89b-12d3-a456-426614174000" },
        account,
      );
      expect(map.uid).toBe("123e4567-e89b-12d3-a456-426614174000");
    });

    it("should handle url fields", async () => {
      const schema = co.map({
        website: z.url(),
      });
      const account = await createJazzTestAccount();
      const map = schema.create({ website: "https://example.com" }, account);
      expect(map.website).toBe("https://example.com");
    });

    it("should handle emoji fields", async () => {
      const schema = co.map({
        emoji: z.emoji(),
      });
      const account = await createJazzTestAccount();
      const map = schema.create({ emoji: "😊" }, account);
      expect(map.emoji).toBe("😊");
    });

    it("should handle base64 fields", async () => {
      const schema = co.map({
        encoded: z.base64(),
      });
      const account = await createJazzTestAccount();
      const map = schema.create({ encoded: "SGVsbG8=" }, account);
      expect(map.encoded).toBe("SGVsbG8=");
    });

    it("should handle base64url fields", async () => {
      const schema = co.map({
        encoded: z.base64url(),
      });
      const account = await createJazzTestAccount();
      const map = schema.create({ encoded: "SGVsbG8-" }, account);
      expect(map.encoded).toBe("SGVsbG8-");
    });
  });

  describe("ID and Network types", () => {
    it("should handle nanoid fields", async () => {
      const schema = co.map({
        uid: z.nanoid(),
      });
      const account = await createJazzTestAccount();
      const map = schema.create({ uid: "V1StGXR8_Z5jdHi6B-myT" }, account);
      expect(map.uid).toBe("V1StGXR8_Z5jdHi6B-myT");
    });

    it("should handle cuid fields", async () => {
      const schema = co.map({
        uid: z.cuid(),
      });
      const account = await createJazzTestAccount();
      const map = schema.create({ uid: "cjld2cjxh0000qzrmn831i7rn" }, account);
      expect(map.uid).toBe("cjld2cjxh0000qzrmn831i7rn");
    });

    it("should handle cuid2 fields", async () => {
      const schema = co.map({
        uid: z.cuid2(),
      });
      const account = await createJazzTestAccount();
      const map = schema.create({ uid: "clg9jv8000000mh8h3j8h3j8h" }, account);
      expect(map.uid).toBe("clg9jv8000000mh8h3j8h3j8h");
    });

    it("should handle ulid fields", async () => {
      const schema = co.map({
        uid: z.ulid(),
      });
      const account = await createJazzTestAccount();
      const map = schema.create({ uid: "01ARZ3NDEKTSV4RRFFQ69G5FAV" }, account);
      expect(map.uid).toBe("01ARZ3NDEKTSV4RRFFQ69G5FAV");
    });

    it("should handle ipv4 fields", async () => {
      const schema = co.map({
        ip: z.ipv4(),
      });
      const account = await createJazzTestAccount();
      const map = schema.create({ ip: "192.168.1.1" }, account);
      expect(map.ip).toBe("192.168.1.1");
    });

    it("should handle ipv6 fields", async () => {
      const schema = co.map({
        ip: z.ipv6(),
      });
      const account = await createJazzTestAccount();
      const map = schema.create(
        { ip: "2001:0db8:85a3:0000:0000:8a2e:0370:7334" },
        account,
      );
      expect(map.ip).toBe("2001:0db8:85a3:0000:0000:8a2e:0370:7334");
    });

    it("should handle cidrv4 fields", async () => {
      const schema = co.map({
        cidr: z.cidrv4(),
      });
      const account = await createJazzTestAccount();
      const map = schema.create({ cidr: "192.168.1.0/24" }, account);
      expect(map.cidr).toBe("192.168.1.0/24");
    });

    it("should handle cidrv6 fields", async () => {
      const schema = co.map({
        cidr: z.cidrv6(),
      });
      const account = await createJazzTestAccount();
      const map = schema.create({ cidr: "2001:db8::/32" }, account);
      expect(map.cidr).toBe("2001:db8::/32");
    });
  });

  describe("ISO and Date types", () => {
    it("should handle iso date fields", async () => {
      const schema = co.map({
        date: z.iso.date(),
      });
      const account = await createJazzTestAccount();
      const map = schema.create({ date: "2024-03-20" }, account);
      expect(map.date).toBe("2024-03-20");
    });

    it("should handle iso time fields", async () => {
      const schema = co.map({
        time: z.iso.time(),
      });
      const account = await createJazzTestAccount();
      const map = schema.create({ time: "14:30:00" }, account);
      expect(map.time).toBe("14:30:00");
    });

    it("should handle iso datetime fields", async () => {
      const schema = co.map({
        datetime: z.iso.datetime(),
      });
      const account = await createJazzTestAccount();
      const map = schema.create({ datetime: "2024-03-20T14:30:00Z" }, account);
      expect(map.datetime).toBe("2024-03-20T14:30:00Z");
    });

    it("should handle iso duration fields", async () => {
      const schema = co.map({
        duration: z.iso.duration(),
      });
      const account = await createJazzTestAccount();
      const map = schema.create({ duration: "P1Y2M3DT4H5M6S" }, account);
      expect(map.duration).toBe("P1Y2M3DT4H5M6S");
    });
  });

  describe("Number and Boolean types", () => {
    it("should handle int fields", async () => {
      const schema = co.map({
        number: z.int(),
      });
      const account = await createJazzTestAccount();
      const map = schema.create({ number: 2147483647 }, account);
      expect(map.number).toBe(2147483647);
    });

    it("should handle int32 fields", async () => {
      const schema = co.map({
        number: z.int32(),
      });
      const account = await createJazzTestAccount();
      const map = schema.create({ number: 2147483647 }, account);
      expect(map.number).toBe(2147483647);
    });

    it("should handle optional fields", async () => {
      const schema = co.map({
        value: z.optional(z.literal("yoda")),
      });
      const account = await createJazzTestAccount();
      const map = schema.create({ value: undefined }, account);
      expect(map.value).toBeUndefined();
    });
  });

  describe("Complex types", () => {
    it("should handle enum fields", async () => {
      const schema = co.map({
        fish: z.enum(["Salmon", "Tuna", "Trout"]),
      });
      const account = await createJazzTestAccount();
      const map = schema.create({ fish: "Salmon" }, account);
      expect(map.fish).toBe("Salmon");
    });

    it("should handle template literal fields", async () => {
      const schema = co.map({
        greeting: z.templateLiteral(["hello, ", z.string()]),
      });
      const account = await createJazzTestAccount();
      const map = schema.create({ greeting: "hello, world" }, account);
      expect(map.greeting).toBe("hello, world");
    });

    it("should handle object fields", async () => {
      const schema = co.map({
        person: z.object({
          name: z.string(),
          age: z.number(),
        }),
      });
      const account = await createJazzTestAccount();
      const map = schema.create({ person: { name: "John", age: 30 } }, account);
      expect(map.person).toEqual({ name: "John", age: 30 });
    });

    it("should handle strict object fields", async () => {
      const schema = co.map({
        person: z.strictObject({
          name: z.string(),
        }),
      });
      const account = await createJazzTestAccount();
      const map = schema.create({ person: { name: "John" } }, account);
      expect(map.person).toEqual({ name: "John" });
    });

    it("should handle tuple fields", async () => {
      const schema = co.map({
        tuple: z.tuple([z.string(), z.number(), z.boolean()]),
      });
      const account = await createJazzTestAccount();
      const map = schema.create({ tuple: ["hello", 42, true] }, account);
      expect(map.tuple).toEqual(["hello", 42, true]);
    });
  });

  describe("Advanced Zod Types", () => {
    it("should handle union types", async () => {
      const schema = co.map({
        value: z.union([z.string(), z.number()]),
      });
      const account = await createJazzTestAccount();
      const map1 = schema.create({ value: "hello" }, account);
      const map2 = schema.create({ value: 42 }, account);
      expect(map1.value).toBe("hello");
      expect(map2.value).toBe(42);
    });

    it("should handle json type", async () => {
      const schema = co.map({
        value: z.json(),
      });
      const account = await createJazzTestAccount();
      const map1 = schema.create({ value: { hello: "world" } }, account);
      const map2 = schema.create({ value: 42 }, account);
      expect(map1.value).toEqual({ hello: "world" });
      expect(map2.value).toBe(42);
    });

    // it("should handle discriminated unions of primitives", async () => {
    //   const schema = co.map({
    //     result: z.discriminatedUnion("status", [
    //       z.object({ status: z.literal("success"), data: z.string() }),
    //       z.object({ status: z.literal("failed"), error: z.string() }),
    //     ]),
    //   });
    //   const account = await createJazzTestAccount();
    //   const successMap = schema.create({ result: { status: "success", data: "data" } }, account);
    //   const failedMap = schema.create({ result: { status: "failed", error: "error" } }, account);
    //   expect(successMap.result).toEqual({ status: "success", data: "data" });
    //   expect(failedMap.result).toEqual({ status: "failed", error: "error" });
    // });

    // it("should handle intersections", async () => {
    //   const schema = co.map({
    //     value: z.intersection(
    //       z.union([z.number(), z.string()]),
    //       z.union([z.number(), z.boolean()])
    //     ),
    //   });
    //   const account = await createJazzTestAccount();
    //   const map = schema.create({ value: 42 }, account);
    //   expect(map.value).toBe(42);
    // });

    // it("should handle record types", async () => {
    //   const schema = co.map({
    //     cache: z.record(z.string(), z.string()),
    //   });
    //   const account = await createJazzTestAccount();
    //   const map = schema.create({ cache: { key1: "value1", key2: "value2" } }, account);
    //   expect(map.cache).toEqual({ key1: "value1", key2: "value2" });
    // });

    it("should handle refined types", async () => {
      const schema = co.map({
        longString: z.string().refine((val) => val.length > 8, {
          error: "Too short!",
        }),
      });
      const account = await createJazzTestAccount();
      const map = schema.create(
        { longString: "this is a long string" },
        account,
      );
      expect(map.longString).toBe("this is a long string");
    });

    it("should log a warning on default values", async () => {
      const consoleSpy = vi.spyOn(console, "warn");

      const schema = co.map({
        fish: z.string().default("tuna"),
      });
      const account = await createJazzTestAccount();
      const map = schema.create(
        {
          fish: "salmon",
        },
        account,
      );
      expect(map.fish).toBe("salmon");

      expect(consoleSpy).toHaveBeenCalledWith(
        "z.default()/z.catch() are not supported in collaborative schemas. They will be ignored.",
      );
      consoleSpy.mockRestore();
    });

    it("should log a warning on catch values", async () => {
      const consoleSpy = vi.spyOn(console, "warn");
      const schema = co.map({
        number: z.number().catch(42),
      });
      const account = await createJazzTestAccount();
      const map = schema.create({ number: 18 }, account);
      expect(map.number).toBe(18);

      expect(consoleSpy).toHaveBeenCalledWith(
        "z.default()/z.catch() are not supported in collaborative schemas. They will be ignored.",
      );
      consoleSpy.mockRestore();
    });

    it("should handle branded types", async () => {
      const schema = co.map({
        cat: z.object({ name: z.string() }).brand<"Cat">(),
      });
      const account = await createJazzTestAccount();
      const map = schema.create({ cat: { name: "Whiskers" } }, account);
      expect(map.cat).toEqual({ name: "Whiskers" });
    });

    it("should handle readonly types", async () => {
      const schema = co.map({
        readonly: z.object({ name: z.string() }).readonly(),
      });
      const account = await createJazzTestAccount();
      const map = schema.create({ readonly: { name: "John" } }, account);
      expect(map.readonly).toEqual({ name: "John" });
    });
  });
});
