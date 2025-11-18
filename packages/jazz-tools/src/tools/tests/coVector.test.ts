import { assert, beforeEach, describe, expect, test, vi } from "vitest";
import { cojsonInternals, Group, isControlledAccount, z } from "../index.js";
import {
  CoVector,
  CoVectorSchema,
  ControlledAccount,
  Loaded,
  co,
} from "../internal.js";
import { createJazzTestAccount, setupJazzTestSync } from "../testing.js";
import { assertLoaded, setupTwoNodes, waitFor } from "./utils.js";

let me: ControlledAccount;

beforeEach(async () => {
  await setupJazzTestSync();
  const account = await createJazzTestAccount({
    isCurrentActiveAccount: true,
    creationProps: { name: "Hermes Puggington" },
  });
  if (!isControlledAccount(account)) {
    throw new Error("account is not a controlled account");
  }
  me = account;
});

const initNodeAndVector = async (
  coVectorSchema: CoVectorSchema,
  vectorInput: number[] | Float32Array = [1, 2, 3],
) => {
  const me = await createJazzTestAccount({
    isCurrentActiveAccount: true,
  });

  const group = Group.create(me);
  group.addMember("everyone", "reader");

  const coVector = coVectorSchema.create(vectorInput, {
    owner: group,
  });

  return { me, coVector };
};

const kbToBytes = (kb: number) => kb * 1024;
const elementsForKb = (kb: number) => kbToBytes(kb) / 4;

describe("Defining a co.vector()", () => {
  test("given a non-positive dimensions count • throws an error", () => {
    expect(() => co.vector(-1)).toThrow(
      "co.vector() expects the vector dimensions count to be a positive integer",
    );
    expect(() => co.vector(0)).toThrow(
      "co.vector() expects the vector dimensions count to be a positive integer",
    );
  });

  test("given a non-integer dimensions count • throws an error", () => {
    expect(() => co.vector(384.5)).toThrow(
      "co.vector() expects the vector dimensions count to be a positive integer",
    );
  });

  test("given a valid dimensions count • creates a CoVector schema", () => {
    const EmbeddingSchema = co.vector(384);
    expect(EmbeddingSchema.builtin).toBe("CoVector");
    expect(EmbeddingSchema.dimensions).toBe(384);
    expect(EmbeddingSchema.create).toBeDefined();
  });
});

describe("Creating a CoVector", async () => {
  const EmbeddingSchema = co.vector(3);
  const { me } = await initNodeAndVector(EmbeddingSchema);

  test("given a vector of mismatched dimensions count • throws an error", () => {
    expect(() => EmbeddingSchema.create([1, 2, 3, 4, 5])).toThrow(
      "Vector dimension mismatch! Expected 3 dimensions, got 5",
    );
  });

  test("directly instantiating CoVector.create • throws an error", () => {
    expect(() => CoVector.create([1, 2, 3])).toThrow(
      "Instantiating CoVector without a dimensions count is not allowed. Use co.vector(...).create() instead.",
    );
  });

  test("given a Array<number> with valid dimensions count • creates a CoVector", () => {
    const embedding = EmbeddingSchema.create([1, 2, 3]);
    expect(embedding).toBeInstanceOf(CoVector);
    expect(embedding[0]).toBe(1);
    expect(embedding[1]).toBe(2);
    expect(embedding[2]).toBe(3);
  });

  test("given a Float32Array with valid dimensions count • creates a CoVector", () => {
    const embedding = EmbeddingSchema.create(new Float32Array([1, 2, 3]));
    expect(embedding).toBeInstanceOf(CoVector);
    expect(embedding[0]).toBe(1);
    expect(embedding[1]).toBe(2);
    expect(embedding[2]).toBe(3);
  });

  test("given Account as owner • creates a CoVector", () => {
    const embedding = EmbeddingSchema.create([1, 2, 3], me);
    expect(embedding.$jazz.owner.myRole()).toEqual("admin");
  });

  test("given Group as owner • creates a CoVector", () => {
    const group = Group.create(me);
    const embedding = EmbeddingSchema.create([1, 2, 3], group);

    expect(embedding.$jazz.owner).toEqual(group);
  });

  describe("nested inside a container CoValue", async () => {
    test("from an array of numbers", () => {
      const VectorMap = co.map({
        embedding: EmbeddingSchema,
      });

      const container = VectorMap.create({
        embedding: [1, 2, 3],
      });

      expect(container.embedding).toBeInstanceOf(CoVector);
      expect(Array.from(container.embedding)).toEqual([1, 2, 3]);
      const vectorOwner = container.embedding.$jazz.owner;
      expect(
        vectorOwner.getParentGroups().map((group) => group.$jazz.id),
      ).toContain(container.$jazz.owner.$jazz.id);

      container.$jazz.set("embedding", [4, 5, 6]);
      expect(Array.from(container.embedding)).toEqual([4, 5, 6]);
    });

    test("from a Float32Array", () => {
      const VectorList = co.list(EmbeddingSchema);

      const list = VectorList.create([new Float32Array([1, 2, 3])]);

      const vector = list[0];
      assert(vector);
      expect(vector).toBeInstanceOf(CoVector);
      expect(Array.from(vector)).toEqual([1, 2, 3]);
      const vectorOwner = vector.$jazz.owner;
      expect(
        vectorOwner.getParentGroups().map((group) => group.$jazz.id),
      ).toContain(list.$jazz.owner.$jazz.id);

      list.$jazz.push(new Float32Array([4, 5, 6]));

      const vector2 = list[1];
      assert(vector2);
      expect(Array.from(vector2)).toEqual([4, 5, 6]);
    });
  });
});

describe("CoVector structure", async () => {
  const EmbeddingSchema = co.vector(5);
  const vector = new Float32Array([1, 2, 3, 4, 5]);
  const coVector = EmbeddingSchema.create(vector);

  test("CoVector keys can be iterated over just like an Float32Array's", () => {
    const keys = [];

    for (const key in coVector) {
      keys.push(key);
    }

    expect(keys).toEqual(Object.keys(vector));
    expect(Object.keys(coVector)).toEqual(Object.keys(vector));
  });

  test("Float32Array can be constructed from a CoVector", () => {
    expect(new Float32Array(coVector)).toEqual(vector);
  });

  test("a CoVector is structurally equal to a Float32Array", () => {
    expect([...coVector]).toEqual([...vector]);
  });

  test("a CoVector identity is NOT equal to a Float32Array", () => {
    expect(coVector).not.toEqual(vector);
  });
});

describe("CoVector reader methods & properties (Float32Array-like)", async () => {
  const EmbeddingSchema = co.vector(5);

  const { me, coVector } = await initNodeAndVector(
    EmbeddingSchema,
    new Float32Array([1, 2, 3, 4, 5]),
  );

  // Properties
  test("has .length", () => {
    expect(coVector.length).toBe(5);
  });
  test("has .buffer", () => {
    expect(coVector.buffer).toEqual(new Float32Array([1, 2, 3, 4, 5]).buffer);
  });
  test("has .byteLength", () => {
    expect(coVector.byteLength).toBe(20);
  });
  test("has .byteOffset", () => {
    expect(coVector.byteOffset).toBe(0);
  });

  test("has index access", () => {
    expect(coVector[0]).toBe(1);
    expect(coVector[1]).toBe(2);
    expect(coVector[2]).toBe(3);
    expect(coVector[3]).toBe(4);
    expect(coVector[4]).toBe(5);
  });

  // Methods
  test("supports .at", () => {
    expect(coVector.at(0)).toBe(1);
    expect(coVector.at(1)).toBe(2);
    expect(coVector.at(2)).toBe(3);
    expect(coVector.at(3)).toBe(4);
    expect(coVector.at(4)).toBe(5);
  });

  test("supports .entries", () => {
    expect(Array.from(coVector.entries())).toEqual([
      [0, 1],
      [1, 2],
      [2, 3],
      [3, 4],
      [4, 5],
    ]);
  });

  test("supports .every", () => {
    expect(coVector.every((item) => item > 0)).toEqual(true);
    expect(coVector.every((item) => item > 3)).toEqual(false);
  });

  test("supports .filter", () => {
    expect(coVector.filter((item) => item > 3)).toEqual(
      new Float32Array([4, 5]),
    );
  });

  test("supports .find", () => {
    expect(coVector.find((item) => item > 3)).toEqual(4);
  });

  test("supports .findIndex", () => {
    expect(coVector.findIndex((item) => item > 3)).toEqual(3);
  });

  test("supports .findLast", () => {
    expect(coVector.findLast((item) => item > 3)).toEqual(5);
  });

  test("supports .findLastIndex", () => {
    expect(coVector.findLastIndex((item) => item > 3)).toEqual(4);
  });

  test("supports .forEach", () => {
    const iterationFn = vi.fn();
    coVector.forEach(iterationFn);

    expect(iterationFn).toHaveBeenCalledTimes(5);

    const f32 = new Float32Array([1, 2, 3, 4, 5]);
    const calls = iterationFn.mock.calls;
    expect(calls.length).toBe(5);
    const expectedValue = [1, 2, 3, 4, 5];
    for (let i = 0; i < 5; i++) {
      const [value, index, arrayArg] = calls[i]!;
      expect(value).toBe(expectedValue[i]);
      expect(index).toBe(i);
      expect(Array.from(arrayArg as any)).toEqual(Array.from(f32));
    }
  });

  test("supports .includes", () => {
    expect(coVector.includes(3)).toBe(true);
    expect(coVector.includes(6)).toBe(false);
  });

  test("supports .indexOf", () => {
    expect(coVector.indexOf(3)).toBe(2);
    expect(coVector.indexOf(6)).toBe(-1);
  });

  test("supports .join", () => {
    expect(coVector.join(":")).toEqual("1:2:3:4:5");
  });

  test("supports .keys", () => {
    expect(Array.from(coVector.keys())).toEqual([0, 1, 2, 3, 4]);
  });

  test("supports .lastIndexOf", () => {
    const coVector = EmbeddingSchema.create([1, 2, 3, 4, 3]);
    expect(coVector.lastIndexOf(3)).toBe(4);
    expect(coVector.lastIndexOf(6)).toBe(-1);
  });

  test("supports .map", () => {
    expect(coVector.map((item) => item * 2)).toEqual(
      new Float32Array([2, 4, 6, 8, 10]),
    );
  });

  test("supports .reduce", () => {
    const reducerFn = vi.fn((acc, item) => acc + item);

    expect(coVector.reduce(reducerFn, 0)).toEqual(15);

    const f32 = new Float32Array([1, 2, 3, 4, 5]);
    const calls = (reducerFn as any).mock.calls as any[];
    expect(calls.length).toBe(5);
    const expectedAcc = [0, 1, 3, 6, 10];
    const expectedVal = [1, 2, 3, 4, 5];
    const expectedIdx = [0, 1, 2, 3, 4];
    for (let i = 0; i < 5; i++) {
      const call = calls[i] as any[];
      expect(call[0]).toBe(expectedAcc[i]);
      expect(call[1]).toBe(expectedVal[i]);
      expect(call[2]).toBe(expectedIdx[i]);
      expect(Array.from(call[3] as any)).toEqual(Array.from(f32));
    }
  });

  test("supports .reduceRight", () => {
    const reducerFn = vi.fn((acc, item) => acc + item);

    expect(coVector.reduceRight(reducerFn, 0)).toEqual(15);

    const f32 = new Float32Array([1, 2, 3, 4, 5]);
    const calls = (reducerFn as any).mock.calls as any[];
    expect(calls.length).toBe(5);
    const expectedAcc = [0, 5, 9, 12, 14];
    const expectedVal = [5, 4, 3, 2, 1];
    const expectedIdx = [4, 3, 2, 1, 0];
    for (let i = 0; i < 5; i++) {
      const call = calls[i] as any[];
      expect(call[0]).toBe(expectedAcc[i]);
      expect(call[1]).toBe(expectedVal[i]);
      expect(call[2]).toBe(expectedIdx[i]);
      expect(Array.from(call[3] as any)).toEqual(Array.from(f32));
    }
  });

  test("supports .slice", () => {
    expect(coVector.slice()).toEqual(new Float32Array([1, 2, 3, 4, 5]));
    expect(coVector.slice(2)).toEqual(new Float32Array([3, 4, 5]));
    expect(coVector.slice(2, 4)).toEqual(new Float32Array([3, 4]));
  });

  test("supports .some", () => {
    expect(coVector.some((item) => item > 3)).toBe(true);
    expect(coVector.some((item) => item > 5)).toBe(false);
  });

  test("supports .subarray", () => {
    expect(coVector.subarray().toString()).toEqual("1,2,3,4,5");
    expect(coVector.subarray(2).toString()).toEqual("3,4,5");
    expect(coVector.subarray(2, 4).toString()).toEqual("3,4");
  });

  test("supports .toJSON (JSON.stringify)", () => {
    expect(JSON.stringify(coVector)).toEqual(`[1,2,3,4,5]`);
  });

  test("supports .toLocaleString", () => {
    expect(coVector.toLocaleString()).toEqual("1,2,3,4,5");
    expect(
      coVector.toLocaleString("ja-JP", { style: "currency", currency: "JPY" }),
    ).toEqual("￥1,￥2,￥3,￥4,￥5");
  });

  test("supports .toReversed", () => {
    expect(coVector.toReversed()).toEqual(new Float32Array([5, 4, 3, 2, 1]));
  });

  test("supports .toSorted", () => {
    const coVector = EmbeddingSchema.create([2, 3, 4, 5, 1]);
    expect(coVector.toSorted()).toEqual(new Float32Array([1, 2, 3, 4, 5]));
  });

  test("supports .toString", () => {
    expect(coVector.toString()).toEqual("1,2,3,4,5");
  });

  test("supports .values", () => {
    expect(Array.from(coVector.values())).toEqual([1, 2, 3, 4, 5]);
  });

  test("supports .with", () => {
    expect(coVector.with(4, 2).toString()).toEqual("1,2,3,4,2");
  });
});

describe("CoVector mutation methods", async () => {
  const EmbeddingSchema = co.vector(5);
  const { coVector } = await initNodeAndVector(
    EmbeddingSchema,
    new Float32Array([1, 2, 3, 4, 5]),
  );

  const expectedErrorMessage = /Cannot mutate a CoVector/i;

  test("calling .copyWithin • throws an error", () => {
    expect(() => coVector.copyWithin(1, 2)).toThrow(expectedErrorMessage);
  });
  test("calling .fill • throws an error", () => {
    expect(() => coVector.fill(1)).toThrow(expectedErrorMessage);
  });
  test("calling .reverse • throws an error", () => {
    expect(() => coVector.reverse()).toThrow(expectedErrorMessage);
  });
  test("calling .set • throws an error", () => {
    expect(() => coVector.set(new Float32Array([6, 7, 8]))).toThrow(
      expectedErrorMessage,
    );
  });
  test("calling .sort • throws an error", () => {
    expect(() => coVector.sort()).toThrow(expectedErrorMessage);
  });
});

describe("Vector calculations on .$jazz", async () => {
  const VectorSchema = co.vector(5);

  const vecA = new Float32Array([1, 2, 3, 4, 5]);
  const vecB = new Float32Array([5, 4, 3, 2, 1]);
  const arrB = [5, 4, 3, 2, 1];

  const coVectorA = VectorSchema.create(vecA);
  const coVectorB = VectorSchema.create(vecB);

  describe("magnitude", () => {
    const magnitudeApprox: [number, number] = [7.4162, 4];

    test("returns the magnitude of the vector", () => {
      expect(coVectorA.$jazz.magnitude()).toBeCloseTo(...magnitudeApprox);
    });
  });

  describe("normalize", () => {
    const normalized = new Float32Array([
      0.1348399668931961, 0.2696799337863922, 0.4045199155807495,
      0.5393598675727844, 0.6741998791694641,
    ]);

    test("returns the normalized vector", () => {
      expect(coVectorA.$jazz.normalize()).toEqual(normalized);
    });
  });

  describe("dot product", () => {
    const dotProduct = 35;

    test("returns the dot product of the 2 vectors", () => {
      expect(coVectorA.$jazz.dotProduct(arrB)).toBe(dotProduct);
      expect(coVectorA.$jazz.dotProduct(vecB)).toBe(dotProduct);
      expect(coVectorA.$jazz.dotProduct(coVectorB)).toBe(dotProduct);
    });
  });

  describe("cosine similarity", () => {
    const similarityApprox: [number, number] = [0.6364, 4];

    test("returns the cosine similarity of the 2 vectors", () => {
      expect(coVectorA.$jazz.cosineSimilarity(arrB)).toBeCloseTo(
        ...similarityApprox,
      );
      expect(coVectorA.$jazz.cosineSimilarity(vecB)).toBeCloseTo(
        ...similarityApprox,
      );
      expect(coVectorA.$jazz.cosineSimilarity(coVectorB)).toBeCloseTo(
        ...similarityApprox,
      );
    });
  });
});

describe("CoVector loading & availability", async () => {
  const EmbeddingSchema = co.vector(3);

  test("when .waitForSync is called • the entire vector is uploaded", async () => {
    const { clientNode, serverNode, clientAccount } = await setupTwoNodes();

    const embedding = EmbeddingSchema.create([1, 2, 3], {
      owner: clientAccount,
    });
    await embedding.$jazz.waitForSync({ timeout: 1000 });

    // Killing the client node so the serverNode can't load the vector from it
    clientNode.gracefulShutdown();

    const loadedEmbedding = await serverNode.load(embedding.$jazz.raw.id);

    expect(loadedEmbedding).not.toBe("unavailable");

    if (loadedEmbedding !== "unavailable") {
      // check that the data was uploaded (3 transactions for smaller vectors: start, data, end)
      expect(loadedEmbedding?.core?.verifiedTransactions.length).toBe(3);
    }
  });

  test("when .waitForSync is called on 1024 kB vector • the entire vector is uploaded", async () => {
    const { clientNode, serverNode, clientAccount } = await setupTwoNodes();

    const kB = 1024;

    const EmbeddingSchema = co.vector(elementsForKb(kB));
    const embedding = EmbeddingSchema.create(
      new Float32Array(elementsForKb(kB)).fill(0.5),
      { owner: clientAccount },
    );
    await embedding.$jazz.waitForSync({ timeout: 1000 });

    // Killing the client node so the serverNode can't load the vector from it
    clientNode.gracefulShutdown();

    const loadedEmbedding = await serverNode.load(embedding.$jazz.raw.id);

    expect(loadedEmbedding).not.toBe("unavailable");

    // check that the data was uploaded
    if (loadedEmbedding !== "unavailable") {
      const expectedTransactionsCount = Math.ceil(
        kbToBytes(kB) /
          cojsonInternals.TRANSACTION_CONFIG.MAX_RECOMMENDED_TX_SIZE,
      );
      expect(loadedEmbedding?.core?.verifiedTransactions.length).toBe(
        expectedTransactionsCount + 2, // +2 for the start and end transactions
      );
    }
  });

  test("when calling .load from different account • syncs across peers", async () => {
    const { coVector } = await initNodeAndVector(
      EmbeddingSchema,
      new Float32Array([9, 8, 7]),
    );

    const alice = await createJazzTestAccount();

    const loadedVector = await EmbeddingSchema.load(coVector.$jazz.id, {
      loadAs: alice,
    });

    assertLoaded(loadedVector);
    expect(loadedVector.length).toBe(3);
    expect(loadedVector[0]).toBe(9);
    expect(loadedVector[1]).toBe(8);
    expect(loadedVector[2]).toBe(7);

    // Check that the cores are not the same...
    expect(loadedVector.$jazz.raw.core).not.toEqual(coVector.$jazz.raw.core);

    // ...but the known states are the same
    expect(loadedVector.$jazz.raw.core.knownState()).toEqual(
      coVector.$jazz.raw.core.knownState(),
    );
    expect(loadedVector.$jazz.raw.core.verifiedTransactions.length).toBe(
      coVector.$jazz.raw.core.verifiedTransactions.length,
    );
  });

  test("when calling .load on a 1024 kB vector from different account • syncs across peers", async () => {
    const kb = 1024;

    const EmbeddingSchema = co.vector(elementsForKb(kb));
    const { coVector } = await initNodeAndVector(
      EmbeddingSchema,
      new Float32Array(elementsForKb(kb)).fill(0.5),
    );

    const alice = await createJazzTestAccount();

    const loadedVector = await EmbeddingSchema.load(coVector.$jazz.id, {
      loadAs: alice,
    });

    assertLoaded(loadedVector);
    expect(loadedVector).toBeInstanceOf(CoVector);
    expect(loadedVector.length).toBe(elementsForKb(kb));
    expect(loadedVector.every((item) => item === 0.5)).toBe(true);

    // Check that the cores are not the same...
    expect(loadedVector.$jazz.raw.core).not.toEqual(coVector.$jazz.raw.core);

    // ...but the known states are the same
    expect(loadedVector.$jazz.raw.core.knownState()).toEqual(
      coVector.$jazz.raw.core.knownState(),
    );
    expect(loadedVector.$jazz.raw.core.verifiedTransactions.length).toBe(
      coVector.$jazz.raw.core.verifiedTransactions.length,
    );
  });
});

describe("CoVector in subscription", async () => {
  const EmbeddingSchema = co.vector(3);
  const DocsChunk = co.map({
    content: z.string(),
    embedding: EmbeddingSchema,
  });
  const Docs = co.list(DocsChunk);

  describe("standalone CoVector", async () => {
    test("subscribing to locally available CoVector • sends update holding the CoVector", async () => {
      const embedding = EmbeddingSchema.create(new Float32Array([10, 20, 30]));

      const updates: Loaded<typeof EmbeddingSchema>[] = [];
      const updatesCallback = vi.fn((embedding) => updates.push(embedding));

      EmbeddingSchema.subscribe(
        embedding.$jazz.id,
        { loadAs: me },
        updatesCallback,
      );

      expect(updatesCallback).not.toHaveBeenCalled();

      await waitFor(() => expect(updatesCallback).toHaveBeenCalled());

      expect(updatesCallback).toHaveBeenCalledTimes(1);

      expect(updates[0]).toBeInstanceOf(CoVector);
      expect(updates[0]?.toString()).toEqual("10,20,30");
    });

    test("subscribing to remotely available CoVector • sends update holding the CoVector", async () => {
      const group = Group.create();
      group.addMember("everyone", "writer");

      const embedding = EmbeddingSchema.create(
        new Float32Array([10, 20, 30]),
        group,
      );

      const userB = await createJazzTestAccount();

      const updates: Loaded<typeof EmbeddingSchema>[] = [];
      const updatesCallback = vi.fn((embedding) => updates.push(embedding));

      EmbeddingSchema.subscribe(
        embedding.$jazz.id,
        { loadAs: userB },
        updatesCallback,
      );

      expect(updatesCallback).not.toHaveBeenCalled();

      await waitFor(() => expect(updatesCallback).toHaveBeenCalled());

      expect(updatesCallback).toHaveBeenCalledTimes(1);

      expect(updates[0]).toBeInstanceOf(CoVector);
      expect(updates[0]?.toString()).toEqual("10,20,30");
    });
  });

  describe("CoVector in CoList[CoMap]", async () => {
    describe("locally available list", async () => {
      test("subscribing with deep resolve • sends update holding the CoVectors", async () => {
        const docs = Docs.create([
          DocsChunk.create({
            content: "Call GET to retrieve document",
            embedding: EmbeddingSchema.create(new Float32Array([1, 2, 3])),
          }),
          DocsChunk.create({
            content: "Call POST to create a new document",
            embedding: EmbeddingSchema.create(new Float32Array([4, 5, 6])),
          }),
        ]);

        const updates: Loaded<typeof Docs, { $each: true }>[] = [];
        const updatesCallback = vi.fn((docs) => updates.push(docs));

        Docs.subscribe(
          docs.$jazz.id,
          { resolve: { $each: true } },
          updatesCallback,
        );

        expect(updatesCallback).not.toHaveBeenCalled();

        await waitFor(() => expect(updatesCallback).toHaveBeenCalled());

        expect(updatesCallback).toHaveBeenCalledTimes(1);

        expect(updates[0]?.[0]?.content).toEqual(
          "Call GET to retrieve document",
        );
        expect(updates[0]?.[1]?.content).toEqual(
          "Call POST to create a new document",
        );
        expect(updates[0]?.[0]?.embedding?.toString()).toBe("1,2,3");
        expect(updates[0]?.[1]?.embedding?.toString()).toBe("4,5,6");

        // Update the second document's embedding
        docs[1]!.$jazz.set(
          "embedding",
          EmbeddingSchema.create(new Float32Array([7, 8, 9])),
        );

        await waitFor(() => expect(updatesCallback).toHaveBeenCalledTimes(2));

        expect(updates[1]?.[0]?.embedding?.toString()).toBe("1,2,3");
        expect(updates[1]?.[1]?.embedding?.toString()).toBe("7,8,9");

        expect(updatesCallback).toHaveBeenCalledTimes(2);
      });

      test("subscribing with autoload • sends update holding the CoVectors", async () => {
        const docs = Docs.create([
          DocsChunk.create({
            content: "Call GET to retrieve document",
            embedding: EmbeddingSchema.create(new Float32Array([1, 2, 3])),
          }),
          DocsChunk.create({
            content: "Call POST to create a new document",
            embedding: EmbeddingSchema.create(new Float32Array([4, 5, 6])),
          }),
        ]);

        const updates: Loaded<typeof Docs>[] = [];
        const updatesCallback = vi.fn((docs) => updates.push(docs));

        Docs.subscribe(docs.$jazz.id, {}, updatesCallback);

        expect(updatesCallback).not.toHaveBeenCalled();

        await waitFor(() => expect(updatesCallback).toHaveBeenCalled());

        expect(updatesCallback).toHaveBeenCalledTimes(1);

        assert(updates[0]?.[0]);
        assertLoaded(updates[0][0]);
        assert(updates[0]?.[1]);
        assertLoaded(updates[0][1]);
        expect(updates[0][0].content).toEqual("Call GET to retrieve document");
        expect(updates[0][1].content).toEqual(
          "Call POST to create a new document",
        );
        expect(updates[0][0].embedding?.toString()).toBe("1,2,3");
        expect(updates[0][1].embedding?.toString()).toBe("4,5,6");

        // Update the second document's embedding
        docs[1]!.$jazz.set(
          "embedding",
          EmbeddingSchema.create(new Float32Array([7, 8, 9])),
        );

        await waitFor(() => expect(updatesCallback).toHaveBeenCalledTimes(2));

        assert(updates[1]?.[0]);
        assertLoaded(updates[1][0]);
        assert(updates[1]?.[1]);
        assertLoaded(updates[1][1]);
        expect(updates[1][0].embedding?.toString()).toBe("1,2,3");
        expect(updates[1][1].embedding?.toString()).toBe("7,8,9");

        expect(updatesCallback).toHaveBeenCalledTimes(2);
      });
    });

    describe("remotely available list", async () => {
      test("subscribing with deep resolve • sends update holding the CoVectors", async () => {
        const group = Group.create({ owner: me });
        group.addMember("everyone", "writer");

        const docs = Docs.create(
          [
            DocsChunk.create(
              {
                content: "Call GET to retrieve document",
                embedding: EmbeddingSchema.create(
                  new Float32Array([1, 2, 3]),
                  group,
                ),
              },
              group,
            ),
            DocsChunk.create(
              {
                content: "Call POST to create a new document",
                embedding: EmbeddingSchema.create(
                  new Float32Array([4, 5, 6]),
                  group,
                ),
              },
              group,
            ),
          ],
          group,
        );

        const userB = await createJazzTestAccount();

        const updates: Loaded<typeof Docs, { $each: true }>[] = [];
        const updatesCallback = vi.fn((docs) => updates.push(docs));

        Docs.subscribe(
          docs.$jazz.id,
          {
            resolve: { $each: true },
            loadAs: userB,
          },
          updatesCallback,
        );

        expect(updatesCallback).not.toHaveBeenCalled();

        await waitFor(() => expect(updatesCallback).toHaveBeenCalled());

        expect(updatesCallback).toHaveBeenCalledTimes(1);

        await waitFor(() => {
          expect(updates[0]?.[0]?.embedding?.toString()).toBe("1,2,3");
          expect(updates[0]?.[1]?.embedding?.toString()).toBe("4,5,6");
        });

        // Update the second document's embedding
        docs[1]!.$jazz.set(
          "embedding",
          EmbeddingSchema.create(new Float32Array([7, 8, 9]), group),
        );

        await waitFor(() => expect(updatesCallback).toHaveBeenCalledTimes(5));

        expect(updates[1]?.[0]?.embedding?.toString()).toBe("1,2,3");
        expect(updates[1]?.[1]?.embedding?.toString()).toBe("7,8,9");

        expect(updatesCallback).toHaveBeenCalledTimes(5);
      });

      test("subscribing with autoload • sends update holding the CoVectors", async () => {
        const group = Group.create({ owner: me });
        group.addMember("everyone", "writer");

        const docs = Docs.create(
          [
            DocsChunk.create(
              {
                content: "Call GET to retrieve document",
                embedding: EmbeddingSchema.create(
                  new Float32Array([1, 2, 3]),
                  group,
                ),
              },
              group,
            ),
            DocsChunk.create(
              {
                content: "Call POST to create a new document",
                embedding: EmbeddingSchema.create(
                  new Float32Array([4, 5, 6]),
                  group,
                ),
              },
              group,
            ),
          ],
          group,
        );

        const userB = await createJazzTestAccount();

        const updates: Loaded<typeof Docs>[] = [];
        const updatesCallback = vi.fn((docs) => updates.push(docs));

        Docs.subscribe(
          docs.$jazz.id,
          {
            loadAs: userB,
          },
          updatesCallback,
        );

        expect(updatesCallback).not.toHaveBeenCalled();

        await waitFor(() => expect(updatesCallback).toHaveBeenCalled());

        await waitFor(() => {
          assert(updates[0]?.[0]);
          assertLoaded(updates[0][0]);
          assert(updates[0]?.[1]);
          assertLoaded(updates[0][1]);
          expect(updates[0][0].embedding?.toString()).toBe("1,2,3");
          expect(updates[0][1].embedding?.toString()).toBe("4,5,6");
        });

        // Update the second document's embedding
        docs[1]!.$jazz.set(
          "embedding",
          EmbeddingSchema.create(new Float32Array([7, 8, 9]), group),
        );

        await waitFor(() => expect(updatesCallback).toHaveBeenCalledTimes(7));

        assert(updates[1]?.[0]);
        assertLoaded(updates[1][0]);
        assert(updates[1]?.[1]);
        assertLoaded(updates[1][1]);
        expect(updates[1][0].embedding?.toString()).toBe("1,2,3");
        expect(updates[1][1].embedding?.toString()).toBe("7,8,9");

        expect(updatesCallback).toHaveBeenCalledTimes(7);
      });
    });
  });
});

describe("CoVector dimension mismatch after loading", async () => {
  test("loading a CoVector with an incompatible schema • throws an error", async () => {
    const { coVector } = await initNodeAndVector(
      co.vector(3),
      new Float32Array([1, 2, 3]),
    );

    await expect(co.vector(9).load(coVector.$jazz.id)).rejects.toThrow(
      /Vector dimension mismatch/,
    );
  });

  test("subscribing to a CoVector with an incompatible schema • throws an error", async () => {
    const EmbeddingSchema = co.vector(3);

    const { coVector } = await initNodeAndVector(
      EmbeddingSchema,
      new Float32Array([1, 2, 3]),
    );

    const updates: Loaded<typeof EmbeddingSchema>[] = [];
    const updatesCallback = vi.fn((embedding) => updates.push(embedding));

    expect(() =>
      co.vector(5).subscribe(coVector.$jazz.id, updatesCallback),
    ).toThrow(/Vector dimension mismatch/);

    expect(updatesCallback).not.toHaveBeenCalled();
  });
});
