import { beforeEach, describe, test, expect } from "vitest";
import { co, z } from "../exports.js";
import { createJazzTestAccount, setupJazzTestSync } from "../testing.js";
import { expectValidationError } from "./utils.js";

describe("runtime validation", () => {
  beforeEach(async () => {
    await setupJazzTestSync();

    await createJazzTestAccount({
      isCurrentActiveAccount: true,
      creationProps: { name: "Hermes Puggington" },
    });
  });

  test("validates numeric fields with composed constraints on create and set", () => {
    const Person = co.map({
      age: z.number().int().min(0).max(120),
    });

    const john = Person.create({ age: 42 });
    expect(john.age).toEqual(42);

    expectValidationError(() => Person.create({ age: -1 }));

    expectValidationError(() => john.$jazz.set("age", 121));

    expectValidationError(() => john.$jazz.set("age", 3.14));
  });

  test("validates string fields with multiple Zod validators", () => {
    const Person = co.map({
      username: z
        .string()
        .min(3)
        .max(10)
        .regex(/^[a-z]+$/),
    });

    const alice = Person.create({ username: "alice" });
    expect(alice.username).toEqual("alice");

    expectValidationError(() => Person.create({ username: "ab" }));

    expectValidationError(() => Person.create({ username: "Alice123" }));

    expectValidationError(() => alice.$jazz.set("username", "bob_1"));
  });

  test("supports optional fields with composed validators", () => {
    const Person = co.map({
      score: z.number().int().min(0).max(100).optional(),
    });

    const player = Person.create({});
    expect(player.score).toBeUndefined();

    player.$jazz.set("score", 50);
    expect(player.score).toEqual(50);

    expectValidationError(() => player.$jazz.set("score", 200));

    player.$jazz.set("score", undefined);
    expect(player.score).toBeUndefined();
  });

  test("validates nested object schemas used as json fields", () => {
    const Settings = z
      .object({
        theme: z.enum(["light", "dark"]).default("light"),
        notifications: z.boolean().optional(),
      })
      .strict();

    const User = co.map({
      settings: Settings,
    });

    const user = User.create({
      settings: { theme: "dark" },
    });

    expect(user.settings.theme).toEqual("dark");

    expectValidationError(() =>
      User.create({
        // @ts-expect-error - invalid enum value
        settings: { theme: "blue" },
      }),
    );

    expectValidationError(() =>
      // @ts-expect-error - invalid enum value at runtime
      user.$jazz.set("settings", { theme: "blue" }),
    );

    user.$jazz.set("settings", {
      theme: "light",
      notifications: true,
    });
    expect(user.settings.theme).toEqual("light");
    expect(user.settings.notifications).toEqual(true);
  });

  test("validates literal, enum, and nullish schemas", () => {
    const Profile = co.map({
      mode: z.literal("auto"),
      role: z.enum(["admin", "user"]),
      nickname: z.string().min(2).nullish(),
    });

    const profile = Profile.create({
      mode: "auto",
      role: "admin",
      nickname: null,
    });

    expect(profile.mode).toEqual("auto");
    expect(profile.nickname).toBeNull();

    expectValidationError(() =>
      Profile.create(
        // @ts-expect-error - literal mismatch
        { mode: "manual", role: "admin" },
      ),
    );

    expectValidationError(() =>
      profile.$jazz.set(
        "role",
        // @ts-expect-error - invalid enum value
        "guest",
      ),
    );

    profile.$jazz.set("nickname", "dj");
    expect(profile.nickname).toEqual("dj");

    profile.$jazz.set("nickname", undefined);
    expect(profile.nickname).toBeUndefined();
  });

  test("applies defaults when values are omitted", () => {
    const Document = co.map({
      title: z.string().min(1).default("Untitled"),
      pageCount: z.number().int().min(1).default(1),
    });

    // @ts-expect-error - missing required fields
    const doc = Document.create({});

    expect(doc.title).toEqual("Untitled");
    expect(doc.pageCount).toEqual(1);

    doc.$jazz.set("title", "Specs");
    doc.$jazz.set("pageCount", 3);
    expect(doc.title).toEqual("Specs");
    expect(doc.pageCount).toEqual(3);

    expectValidationError(() => doc.$jazz.set("pageCount", 0));
  });

  test("validates string formats and identifiers", () => {
    const Contact = co.map({
      email: z.email(),
      website: z.url(),
      userId: z.uuid(),
    });

    const contact = Contact.create({
      email: "user@example.com",
      website: "https://example.com",
      userId: "123e4567-e89b-12d3-a456-426614174000",
    });

    expect(contact.website).toEqual("https://example.com");

    expectValidationError(() =>
      Contact.create({
        email: "not-email",
        website: "https://example.com",
        userId: "123",
      }),
    );

    expectValidationError(() => contact.$jazz.set("website", "notaurl"));

    expectValidationError(() => contact.$jazz.set("userId", "not-a-uuid"));
  });

  test("validates arrays and tuples", () => {
    const Metrics = co.map({
      tags: z.array(z.string().min(1)).min(1),
      coordinates: z.tuple([z.number().int(), z.number().int()]),
    });

    const metrics = Metrics.create({
      tags: ["alpha", "beta"],
      coordinates: [10, 20],
    });

    expect(metrics.tags).toEqual(["alpha", "beta"]);

    expectValidationError(() =>
      Metrics.create(
        // @ts-expect-error - empty tags and wrong tuple length
        { tags: [], coordinates: [10, 20, 30] },
      ),
    );

    expectValidationError(() => metrics.$jazz.set("tags", ["", "beta"]));

    expectValidationError(() => metrics.$jazz.set("coordinates", [10.5, 20]));
  });

  test("validates unions and discriminated unions", () => {
    const Shape = co.map({
      size: z.union([z.literal("small"), z.literal("large")]),
      item: z.discriminatedUnion("kind", [
        z.object({ kind: z.literal("text"), value: z.string().min(1) }),
        z.object({ kind: z.literal("count"), value: z.number().int().min(1) }),
      ]),
    });

    const shape = Shape.create({
      size: "small",
      item: { kind: "text", value: "hello" },
    });

    expect(shape.size).toEqual("small");
    expect(shape.item.kind).toEqual("text");

    expectValidationError(() =>
      Shape.create({
        // @ts-expect-error - invalid union member
        size: "medium",
        item: { kind: "text", value: "hello" },
      }),
    );

    expectValidationError(() =>
      shape.$jazz.set("item", { kind: "count", value: 0 }),
    );
  });

  test("applies refine checks on complex schemas", () => {
    const Credentials = co.map({
      password: z
        .string()
        .min(8)
        .refine(
          (value) =>
            /[A-Z]/.test(value) &&
            /[a-z]/.test(value) &&
            /\d/.test(value) &&
            /[^A-Za-z0-9]/.test(value),
        ),
    });

    const credentials = Credentials.create({
      password: "GoodPa$$w0rd",
    });

    expect(credentials.password).toEqual("GoodPa$$w0rd");

    expectValidationError(() => Credentials.create({ password: "password" }));

    expectValidationError(() => credentials.$jazz.set("password", "NoDigits!"));
  });

  test("skips runtime validation for fields when validation is loose", () => {
    const Person = co.map({
      age: z.number().int().min(0),
    });

    const john = Person.create({ age: 10 });

    expect(() =>
      john.$jazz.set("age", -5, { validation: "loose" }),
    ).not.toThrow();

    expect(john.age).toEqual(-5);
  });
});
