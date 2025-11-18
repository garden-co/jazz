import { assert, describe, expect, it } from "vitest";
import { createJazzTestAccount, setupJazzTestSync } from "jazz-tools/testing";
import { co, z } from "jazz-tools";
import { restoreCoMapToTimestamp } from "../../utils/history";
import { JsonObject } from "cojson";
import { loadCoValueOrFail, waitFor } from "../../../tools/tests/utils";

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

describe("restoreCoMapToTimestamp", async () => {
  const account = await setupJazzTestSync();

  it("should restore CoMap to a previous timestamp", async () => {
    const value = co
      .map({
        pet: z.string(),
        age: z.number(),
      })
      .create({ pet: "dog", age: 10 }, account);

    await sleep(2);
    value.$jazz.set("pet", "cat");
    value.$jazz.set("age", 20);

    const timestamps = value.$jazz.raw.core.verifiedTransactions.map(
      (tx) => tx.madeAt,
    );
    const initialTimestamp = timestamps[0]!;

    expect(value.pet).toBe("cat");
    expect(value.age).toBe(20);

    restoreCoMapToTimestamp(value.$jazz.raw, initialTimestamp, false);

    expect(value.pet).toBe("dog");
    expect(value.age).toBe(10);
  });

  it("should restore single property change", async () => {
    const value = co
      .map({
        pet: z.string(),
      })
      .create({ pet: "dog" }, account);

    await sleep(2);
    value.$jazz.set("pet", "cat");

    const timestamps = value.$jazz.raw.core.verifiedTransactions.map(
      (tx) => tx.madeAt,
    );
    const initialTimestamp = timestamps[0]!;

    expect(value.pet).toBe("cat");

    restoreCoMapToTimestamp(value.$jazz.raw, initialTimestamp, false);

    expect(value.pet).toBe("dog");
  });

  it("should restore multiple property changes at different times", async () => {
    const value = co
      .map({
        a: z.string(),
        b: z.string(),
        c: z.string(),
      })
      .create({ a: "1", b: "2", c: "3" }, account);

    await sleep(2);
    value.$jazz.set("a", "4");
    await sleep(2);
    value.$jazz.set("b", "5");
    await sleep(2);
    value.$jazz.set("c", "6");

    const timestamps = value.$jazz.raw.core.verifiedTransactions.map(
      (tx) => tx.madeAt,
    );
    const initialTimestamp = timestamps[0]!;

    expect(value.a).toBe("4");
    expect(value.b).toBe("5");
    expect(value.c).toBe("6");

    restoreCoMapToTimestamp(value.$jazz.raw, initialTimestamp, false);

    expect(value.a).toBe("1");
    expect(value.b).toBe("2");
    expect(value.c).toBe("3");
  });

  it("should not remove unknown properties when flag is false", async () => {
    const value = co
      .map({
        pet: z.string(),
        age: z.number().optional(),
      })
      .create({ pet: "dog" }, account);

    await sleep(2);
    value.$jazz.set("age", 10);
    await sleep(2);
    value.$jazz.set("pet", "cat");

    const timestamps = value.$jazz.raw.core.verifiedTransactions.map(
      (tx) => tx.madeAt,
    );
    const initialTimestamp = timestamps[0]!;

    expect(value.pet).toBe("cat");
    expect(value.age).toBe(10);

    restoreCoMapToTimestamp(value.$jazz.raw, initialTimestamp, false);

    expect(value.pet).toBe("dog");
    expect(value.age).toBe(10);
  });

  it("should remove unknown properties when flag is true", async () => {
    const value = co
      .map({
        pet: z.string(),
        age: z.number().optional(),
      })
      .create({ pet: "dog" }, account);

    await sleep(2);
    value.$jazz.set("age", 10);
    await sleep(2);
    value.$jazz.set("pet", "cat");

    const timestamps = value.$jazz.raw.core.verifiedTransactions.map(
      (tx) => tx.madeAt,
    );
    const initialTimestamp = timestamps[0]!;

    expect(value.pet).toBe("cat");
    expect(value.age).toBe(10);

    restoreCoMapToTimestamp(value.$jazz.raw, initialTimestamp, true);

    expect(value.pet).toBe("dog");
    expect(value.age).toBeUndefined();
  });

  it("should remove multiple unknown properties when flag is true", async () => {
    const value = co
      .map({
        a: z.string(),
        b: z.string().optional(),
        c: z.string().optional(),
      })
      .create({ a: "1" }, account);

    await sleep(2);
    value.$jazz.set("b", "2");
    await sleep(2);
    value.$jazz.set("c", "3");
    await sleep(2);
    value.$jazz.set("a", "4");

    const timestamps = value.$jazz.raw.core.verifiedTransactions.map(
      (tx) => tx.madeAt,
    );
    const initialTimestamp = timestamps[0]!;

    expect(value.a).toBe("4");
    expect(value.b).toBe("2");
    expect(value.c).toBe("3");

    restoreCoMapToTimestamp(value.$jazz.raw, initialTimestamp, true);

    expect(value.a).toBe("1");
    expect(value.b).toBeUndefined();
    expect(value.c).toBeUndefined();
  });

  it("should handle empty CoMap", async () => {
    const value = co.map({}).create({}, account);

    const timestamps = value.$jazz.raw.core.verifiedTransactions.map(
      (tx) => tx.madeAt,
    );
    const initialTimestamp = timestamps[0]!;

    const currentState = value.$jazz.raw.toJSON() as JsonObject;
    expect(Object.keys(currentState).length).toBe(0);

    restoreCoMapToTimestamp(value.$jazz.raw, initialTimestamp, false);

    const restoredState = value.$jazz.raw.toJSON() as JsonObject;
    expect(Object.keys(restoredState).length).toBe(0);
  });

  it("should handle restoring to same state (no changes)", async () => {
    const value = co
      .map({
        pet: z.string(),
      })
      .create({ pet: "dog" }, account);

    const timestamps = value.$jazz.raw.core.verifiedTransactions.map(
      (tx) => tx.madeAt,
    );
    const latestTimestamp = timestamps[timestamps.length - 1]!;

    expect(value.pet).toBe("dog");

    restoreCoMapToTimestamp(value.$jazz.raw, latestTimestamp, false);

    expect(value.pet).toBe("dog");
  });

  it("should handle complex data types", async () => {
    const value = co
      .map({
        obj: z.object({
          name: z.string(),
          count: z.number(),
        }),
        date: z.date(),
        bool: z.boolean(),
      })
      .create(
        {
          obj: { name: "test", count: 42 },
          date: new Date("2024-01-01"),
          bool: true,
        },
        account,
      );

    await sleep(2);
    value.$jazz.set("obj", { name: "updated", count: 100 });
    value.$jazz.set("bool", false);

    const timestamps = value.$jazz.raw.core.verifiedTransactions.map(
      (tx) => tx.madeAt,
    );
    const initialTimestamp = timestamps[0]!;

    expect(value.obj.name).toBe("updated");
    expect(value.obj.count).toBe(100);
    expect(value.bool).toBe(false);

    restoreCoMapToTimestamp(value.$jazz.raw, initialTimestamp, false);

    expect(value.obj.name).toBe("test");
    expect(value.obj.count).toBe(42);
    expect(value.bool).toBe(true);
  });

  it("should handle restoring to intermediate timestamp", async () => {
    const value = co
      .map({
        counter: z.number(),
      })
      .create({ counter: 1 }, account);

    await sleep(2);
    value.$jazz.set("counter", 2);
    await sleep(2);
    value.$jazz.set("counter", 3);
    await sleep(2);
    value.$jazz.set("counter", 4);

    const timestamps = value.$jazz.raw.core.verifiedTransactions.map(
      (tx) => tx.madeAt,
    );
    const intermediateTimestamp = timestamps[2]!;

    expect(value.counter).toBe(4);

    restoreCoMapToTimestamp(value.$jazz.raw, intermediateTimestamp, false);

    expect(value.counter).toBe(3);
  });

  it("should handle null value at timestamp (should return early)", async () => {
    const value = co
      .map({
        pet: z.string(),
      })
      .create({ pet: "dog" }, account);

    const invalidTimestamp = 0;

    const beforeRestore = value.pet;

    restoreCoMapToTimestamp(value.$jazz.raw, invalidTimestamp, false);

    expect(value.pet).toBe(beforeRestore);
  });

  it("should update changed properties correctly", async () => {
    const value = co
      .map({
        pet: z.string(),
        age: z.number(),
      })
      .create({ pet: "dog", age: 10 }, account);

    await sleep(2);
    value.$jazz.set("pet", "cat");
    value.$jazz.set("age", 20);

    const timestamps = value.$jazz.raw.core.verifiedTransactions.map(
      (tx) => tx.madeAt,
    );
    const initialTimestamp = timestamps[0]!;

    restoreCoMapToTimestamp(value.$jazz.raw, initialTimestamp, false);

    const restoredState = value.$jazz.raw.toJSON() as JsonObject;
    expect(restoredState.pet).toBe("dog");
    expect(restoredState.age).toBe(10);
  });

  it("should handle partial property updates", async () => {
    const value = co
      .map({
        a: z.string(),
        b: z.string(),
        c: z.string(),
      })
      .create({ a: "1", b: "2", c: "3" }, account);

    await sleep(2);
    value.$jazz.set("a", "4");

    const timestamps = value.$jazz.raw.core.verifiedTransactions.map(
      (tx) => tx.madeAt,
    );
    const initialTimestamp = timestamps[0]!;

    expect(value.a).toBe("4");
    expect(value.b).toBe("2");
    expect(value.c).toBe("3");

    restoreCoMapToTimestamp(value.$jazz.raw, initialTimestamp, false);

    expect(value.a).toBe("1");
    expect(value.b).toBe("2");
    expect(value.c).toBe("3");
  });

  it("should handle restoring as writer account", async () => {
    const writer = await createJazzTestAccount();
    const groupOnAdmin = co.group().create({ owner: account });
    groupOnAdmin.addMember(writer, "writer");

    const schema = co.map({
      pet: z.string(),
    });

    const value = schema.create({ pet: "dog" }, groupOnAdmin);

    await sleep(2);
    value.$jazz.set("pet", "cat");

    const initialTimestamp =
      value.$jazz.raw.core.verifiedTransactions.at(0)!.madeAt;

    const valueOnWriter = await schema.load(value.$jazz.id, {
      loadAs: writer,
    });
    assert(valueOnWriter.$isLoaded);

    restoreCoMapToTimestamp(valueOnWriter.$jazz.raw, initialTimestamp, false);

    expect(valueOnWriter.pet).toBe("dog");
  });

  it("should not restore if account cannot write", async () => {
    const reader = await createJazzTestAccount();
    const groupOnAdmin = co.group().create({ owner: account });
    groupOnAdmin.addMember(reader, "reader");

    const schema = co.map({
      pet: z.string(),
    });

    const value = schema.create({ pet: "dog" }, groupOnAdmin);

    await sleep(2);
    value.$jazz.set("pet", "cat");

    const initialTimestamp =
      value.$jazz.raw.core.verifiedTransactions.at(0)!.madeAt;

    const valueOnReader = await schema.load(value.$jazz.id, {
      loadAs: reader,
    });
    assert(valueOnReader.$isLoaded);

    restoreCoMapToTimestamp(valueOnReader.$jazz.raw, initialTimestamp, false);

    expect(valueOnReader.pet).toBe("cat");
  });
});
