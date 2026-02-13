import { beforeEach, describe, expect, test } from "vitest";
import { CoPlainText, FileStream, co, z } from "../exports.js";
import { createJazzTestAccount, setupJazzTestSync } from "../testing.js";

describe("co.optional", () => {
  beforeEach(async () => {
    await setupJazzTestSync();

    const account = await createJazzTestAccount({
      isCurrentActiveAccount: true,
      creationProps: { name: "Hermes Puggington" },
    });
    account.$jazz.set("root", {});
  });

  test("can use co.optional with CoValue schemas as values", () => {
    const Person = co.map({
      preferredName: co.optional(co.plainText()),
    });

    const person = Person.create({});

    type ExpectedType = {
      preferredName?: CoPlainText;
    };

    function matches(value: ExpectedType) {
      return value;
    }

    matches(person);
  });

  test("cannot use co.optional with zod schemas as values", () => {
    const Person = co.map({
      // @ts-expect-error: cannot use co.optional with a Zod schema
      preferredName: co.optional(z.string()),
    });
  });

  test("can use schema.optional() on all CoValue schemas but co.optional()", async () => {
    const Option1 = co.map({ type: z.literal("1") });
    const Option2 = co.map({ type: z.literal("2") });
    const Schema = co.map({
      plainText: co.plainText().optional(),
      richText: co.richText().optional(),
      fileStream: co.fileStream().optional(),
      image: co.image().optional(),
      record: co.record(z.string(), z.string()).optional(),
      map: co.map({ field: z.string() }).optional(),
      list: co.list(z.string()).optional(),
      feed: co.feed(z.string()).optional(),
      account: co.account().optional(),
      union: co.discriminatedUnion("type", [Option1, Option2]).optional(),
    });

    const schema = Schema.create({});

    expect(schema.plainText).toBeUndefined();
    expect(schema.richText).toBeUndefined();
    expect(schema.fileStream).toBeUndefined();
    expect(schema.image).toBeUndefined();
    expect(schema.record).toBeUndefined();
    expect(schema.map).toBeUndefined();
    expect(schema.list).toBeUndefined();
    expect(schema.feed).toBeUndefined();
    expect(schema.account).toBeUndefined();
    expect(schema.union).toBeUndefined();

    schema.$jazz.set(
      "plainText",
      Schema.shape.plainText.innerType.create("Hello"),
    );
    schema.$jazz.set(
      "richText",
      Schema.shape.richText.innerType.create("Hello"),
    );
    schema.$jazz.set("fileStream", Schema.shape.fileStream.innerType.create());
    schema.$jazz.set(
      "image",
      Schema.shape.image.innerType.create({
        originalSize: [1920, 1080],
        original: FileStream.create(),
        progressive: false,
      }),
    );
    schema.$jazz.set(
      "record",
      Schema.shape.record.innerType.create({ field: "hello" }),
    );
    schema.$jazz.set(
      "map",
      Schema.shape.map.innerType.create({ field: "hello" }),
    );
    schema.$jazz.set("list", Schema.shape.list.innerType.create([]));
    schema.$jazz.set("feed", Schema.shape.feed.innerType.create([]));
    const loadedAccount = await co
      .account()
      .getMe()
      .$jazz.ensureLoaded({ resolve: { profile: true, root: true } });
    schema.$jazz.set("account", loadedAccount);
    schema.$jazz.set("union", Option1.create({ type: "1" }));

    expect(schema.plainText?.toString()).toEqual("Hello");
    expect(schema.richText?.toString()).toEqual("Hello");
    expect(schema.fileStream).not.toBeUndefined();
    expect(schema.image?.originalSize).toEqual([1920, 1080]);
    expect(schema.record?.field).toEqual("hello");
    expect(schema.map?.field).toEqual("hello");
    expect(schema.list).toEqual([]);
    expect(schema.feed).not.toBeUndefined();
    expect(schema.account?.profile.name).toEqual("Hermes Puggington");
    expect(schema.union?.type).toEqual("1");
  });

  test("can access the inner schema of a co.optional", () => {
    const Person = co.map({
      preferredName: co.optional(co.plainText()),
    });

    const person = Person.create({
      preferredName: Person.shape["preferredName"].innerType.create("John"),
    });

    expect(person?.preferredName?.toString()).toEqual("John");
  });

  test("can set undefined to an optional field", () => {
    const Person = co.map({
      name: co.optional(co.plainText()),
    });

    const person = Person.create({});

    person.$jazz.set("name", undefined);

    expect(person.name).toBeUndefined();
  });
});
