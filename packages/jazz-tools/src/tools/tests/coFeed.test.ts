import { WasmCrypto } from "cojson/crypto/WasmCrypto";
import { Channel } from "queueueue";
import {
  assert,
  beforeEach,
  describe,
  expect,
  expectTypeOf,
  test,
} from "vitest";
import {
  Account,
  FileStream,
  Group,
  co,
  isControlledAccount,
  z,
} from "../index.js";
import { createJazzTestAccount, setupJazzTestSync } from "../testing.js";
import { setupTwoNodes, waitFor } from "./utils.js";
import {
  CoFeed,
  CoFeedInstanceCoValuesNullable,
  ControlledAccount,
  TypeSym,
} from "../internal.js";

const Crypto = await WasmCrypto.create();

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

describe("Simple CoFeed operations", async () => {
  let TestStream: co.Feed<z.z.ZodString>;
  let stream: CoFeed<string>;

  beforeEach(async () => {
    TestStream = co.feed(z.string());
    stream = TestStream.create(["milk"], { owner: me });
  });

  test("Construction", () => {
    expect(stream.perAccount[me.$jazz.id]?.value).toEqual("milk");
    expect(stream.perSession[me.$jazz.sessionID]?.value).toEqual("milk");
  });

  describe("Create CoFeed with a reference", () => {
    test("using a CoValue", () => {
      const Text = co.plainText();
      const TextStream = co.feed(Text);

      const stream = TextStream.create([Text.create("milk")], {
        owner: me,
      });

      const coValue = stream.perAccount[me.$jazz.id]?.value;
      expect(coValue?.toString()).toEqual("milk");
    });

    describe("using JSON", () => {
      test("automatically creates CoValues for nested objects", () => {
        const Text = co.plainText();
        const TextStream = co.feed(Text);

        const stream = TextStream.create(["milk"], { owner: me });

        const coValue = stream.perAccount[me.$jazz.id]?.value;
        expect(coValue?.toString()).toEqual("milk");
      });

      test("can create a coPlainText from an empty string", () => {
        const Schema = co.feed(co.plainText());
        const feed = Schema.create([""]);
        expect(feed.perAccount[me.$jazz.id]?.value?.toString()).toBe("");
      });
    });
  });

  test("Construction with nullable values", () => {
    const NullableTestStream = co.feed(z.string().nullable());
    const stream = NullableTestStream.create(["milk", null], { owner: me });

    expect(stream.perAccount[me.$jazz.id]?.value).toEqual(null);
    expect(stream.perSession[me.$jazz.sessionID]?.value).toEqual(null);
  });

  test("Construction with an Account", () => {
    const stream = TestStream.create(["milk"], me);

    expect(stream.perAccount[me.$jazz.id]?.value).toEqual("milk");
    expect(stream.perSession[me.$jazz.sessionID]?.value).toEqual("milk");
  });

  test("Construction with a Group", () => {
    const group = Group.create(me);
    const stream = TestStream.create(["milk"], group);

    expect(stream.perAccount[me.$jazz.id]?.value).toEqual("milk");
    expect(stream.perSession[me.$jazz.sessionID]?.value).toEqual("milk");
  });

  test("toJSON", () => {
    expect(stream.toJSON()).toEqual({
      $jazz: { id: stream.$jazz.id },
      in: {
        [me.$jazz.sessionID]: stream.perSession[me.$jazz.sessionID]?.value,
      },
    });
  });

  describe("Mutation", () => {
    test("push element into CoFeed of non-collaborative values", () => {
      stream.$jazz.push("bread");
      expect(stream.perAccount[me.$jazz.id]?.value).toEqual("bread");
      expect(stream.perSession[me.$jazz.sessionID]?.value).toEqual("bread");

      stream.$jazz.push("butter");
      expect(stream.perAccount[me.$jazz.id]?.value).toEqual("butter");
      expect(stream.perSession[me.$jazz.sessionID]?.value).toEqual("butter");
    });

    test("push CoValue into CoFeed of CoValues", () => {
      const Schema = co.feed(co.plainText());
      const stream = Schema.create(["milk"]);
      stream.$jazz.push(Schema.element.create("bread"));
      expect(stream.perAccount[me.$jazz.id]?.value?.toString()).toEqual(
        "bread",
      );
      expect(stream.perSession[me.$jazz.sessionID]?.value?.toString()).toEqual(
        "bread",
      );
    });

    test("push JSON into CoFeed of CoValues", () => {
      const Schema = co.feed(co.plainText());
      const stream = Schema.create(["milk"]);
      stream.$jazz.push("bread");
      expect(stream.perAccount[me.$jazz.id]?.value?.toString()).toEqual(
        "bread",
      );
      expect(stream.perSession[me.$jazz.sessionID]?.value?.toString()).toEqual(
        "bread",
      );
    });
  });
});

describe("CoFeed resolution", async () => {
  const TwiceNestedStream = co.feed(z.string());
  const NestedStream = co.feed(TwiceNestedStream);
  const TestStream = co.feed(NestedStream);

  const initNodeAndStream = async () => {
    const me = await createJazzTestAccount({
      isCurrentActiveAccount: true,
    });

    const group = Group.create(me);
    group.makePublic();
    const stream = TestStream.create(
      [
        NestedStream.create(
          [TwiceNestedStream.create(["milk"], { owner: group })],
          { owner: group },
        ),
      ],
      { owner: group },
    );

    return { me, stream };
  };

  test("Construction", async () => {
    const { me, stream } = await initNodeAndStream();

    // TODO: fix this
    // expectTypeOf(stream[me.id]).not.toBeAny();

    expect(
      stream.perAccount[me.$jazz.id]?.value?.perAccount[me.$jazz.id]?.value
        ?.perAccount[me.$jazz.id]?.value,
    ).toEqual("milk");
  });

  test("Loading and availability", async () => {
    const { me, stream } = await initNodeAndStream();

    const anotherAccount = await createJazzTestAccount();

    const loadedStream = await TestStream.load(stream.$jazz.id, {
      loadAs: anotherAccount,
    });

    assert(loadedStream);

    const myStream = loadedStream.perAccount[me.$jazz.id];

    assert(myStream);

    await waitFor(() => expect(myStream.value).toBeTruthy());

    assert(myStream.value);

    const loadedNestedStreamByMe = myStream.value.perAccount[me.$jazz.id];

    assert(loadedNestedStreamByMe);

    await waitFor(() => expect(loadedNestedStreamByMe.value).toBeTruthy());

    assert(loadedNestedStreamByMe.value);

    const loadedTwiceNestedStreamByMe =
      loadedNestedStreamByMe.value.perAccount[me.$jazz.id];

    assert(loadedTwiceNestedStreamByMe);

    await waitFor(() => expect(loadedTwiceNestedStreamByMe.value).toBe("milk"));

    assert(loadedTwiceNestedStreamByMe.value);
  });

  test("Subscription & auto-resolution", async () => {
    const { me, stream } = await initNodeAndStream();
    const accountId = me.$jazz.id;

    const anotherAccount = await createJazzTestAccount();

    let result: CoFeedInstanceCoValuesNullable<co.Feed<co.Feed<z.z.ZodString>>>;

    TestStream.subscribe(
      stream.$jazz.id,
      { loadAs: anotherAccount },
      (subscribedStream) => {
        result = subscribedStream;
      },
    );

    await waitFor(() => expect(result).toBeDefined());

    await waitFor(() => {
      expect(
        result.perAccount[accountId]?.value?.perAccount[accountId]?.value
          ?.perAccount[accountId]?.value,
      ).toBe("milk");
    });

    // When assigning a new nested stream, we get an update
    const newTwiceNested = TwiceNestedStream.create(["butter"], {
      owner: stream.$jazz.owner,
    });

    const newNested = NestedStream.create([newTwiceNested], {
      owner: stream.$jazz.owner,
    });

    stream.$jazz.push(newNested);

    await waitFor(() => {
      expect(
        result.perAccount[accountId]?.value?.perAccount[accountId]?.value
          ?.perAccount[accountId]?.value,
      ).toBe("butter");
    });

    // we get updates when the new nested stream changes
    newTwiceNested.$jazz.push("jam");

    await waitFor(() => {
      expect(
        result.perAccount[accountId]?.value?.perAccount[accountId]?.value
          ?.perAccount[accountId]?.value,
      ).toBe("jam");
    });
  });

  test("Subscription without options", async () => {
    const { me, stream } = await initNodeAndStream();

    const queue = new Channel();

    TestStream.subscribe(stream.$jazz.id, (subscribedStream) => {
      void queue.push(subscribedStream);
    });

    const update1 = (await queue.next()).value;
    expect(
      update1.perAccount[me.$jazz.id]?.value?.perAccount[me.$jazz.id]?.value
        ?.perAccount[me.$jazz.id]?.value,
    ).toBe("milk");

    stream.perAccount[me.$jazz.id]!.value!.perAccount[
      me.$jazz.id
    ]!.value!.$jazz.push("bread");

    const update2 = (await queue.next()).value;
    expect(
      update2.perAccount[me.$jazz.id]?.value?.perAccount[me.$jazz.id]?.value
        ?.perAccount[me.$jazz.id]?.value,
    ).toBe("bread");
  });
});

describe("Simple FileStream operations", async () => {
  const me = await Account.create({
    creationProps: { name: "Hermes Puggington" },
    crypto: Crypto,
  });

  const stream = FileStream.create({ owner: me });

  describe("FileStream", () => {
    test("Construction", () => {
      expect(stream.getChunks()).toBe(undefined);
    });

    test("toJSON", () => {
      stream.start({ mimeType: "text/plain" });
      stream.push(new Uint8Array([1, 2, 3]));
      stream.push(new Uint8Array([4, 5, 6]));
      stream.end();

      expect(stream.toJSON()).toEqual({
        $jazz: { id: stream.$jazz.id },
        mimeType: "text/plain",
        chunks: [new Uint8Array([1, 2, 3]), new Uint8Array([4, 5, 6])],
        filename: undefined,
        finished: true,
        totalSizeBytes: undefined,
      });
    });

    test("Mutation", () => {
      stream.start({ mimeType: "text/plain" });
      stream.push(new Uint8Array([1, 2, 3]));
      stream.push(new Uint8Array([4, 5, 6]));
      stream.end();

      const chunks = stream.getChunks();
      expect(chunks?.mimeType).toBe("text/plain");
      expect(chunks?.chunks).toEqual([
        new Uint8Array([1, 2, 3]),
        new Uint8Array([4, 5, 6]),
      ]);
      expect(chunks?.finished).toBe(true);
    });
  });

  describe("co.fileStream", () => {
    const fs = co.fileStream().create({ owner: me });

    test("Construction", () => {
      expect(fs.getChunks()).toBe(undefined);
    });

    test("Type compatibility", () => {
      // Check base functionality works
      expectTypeOf(co.fileStream()).toHaveProperty("create");

      // We can acknowledge the type error exists
      // This is a runtime test that verifies that despite the TypeScript error,
      // the functionality still works as expected
      expect(typeof fs.getChunks).toBe("function");
    });

    test("Mutation", () => {
      fs.start({ mimeType: "text/plain" });
      fs.push(new Uint8Array([1, 2, 3]));
      fs.push(new Uint8Array([4, 5, 6]));
      fs.end();
    });
  });
});

describe("FileStream loading & Subscription", async () => {
  const initNodeAndStream = async () => {
    const me = await createJazzTestAccount({
      isCurrentActiveAccount: true,
    });

    const group = Group.create(me);
    group.makePublic();
    const stream = FileStream.create({ owner: group });

    stream.start({ mimeType: "text/plain" });
    stream.push(new Uint8Array([1, 2, 3]));
    stream.push(new Uint8Array([4, 5, 6]));
    stream.end();

    return { me, stream };
  };

  test("Construction", async () => {
    const { stream } = await initNodeAndStream();
    expect(stream.getChunks()).toEqual({
      mimeType: "text/plain",
      chunks: [new Uint8Array([1, 2, 3]), new Uint8Array([4, 5, 6])],
      finished: true,
    });
  });

  test("Loading and availability", async () => {
    const { stream } = await initNodeAndStream();
    const anotherAccount = await createJazzTestAccount();

    const loadedStream = await FileStream.load(stream.$jazz.id, {
      loadAs: anotherAccount,
    });

    expect(loadedStream?.getChunks()).toEqual({
      mimeType: "text/plain",
      chunks: [new Uint8Array([1, 2, 3]), new Uint8Array([4, 5, 6])],
      finished: true,
    });
  });

  test("Subscription", async () => {
    const { me } = await initNodeAndStream();
    const group = Group.create(me);
    group.makePublic();
    const stream = FileStream.create({ owner: group });

    const anotherAccount = await createJazzTestAccount();

    const queue = new Channel();

    FileStream.subscribe(
      stream.$jazz.id,
      { loadAs: anotherAccount },
      (subscribedStream) => {
        void queue.push(subscribedStream);
      },
    );

    const update1 = (await queue.next()).value;
    expect(update1.getChunks()).toBe(undefined);

    stream.start({ mimeType: "text/plain" });

    const update2 = (await queue.next()).value;
    expect(update2.getChunks({ allowUnfinished: true })).toEqual({
      mimeType: "text/plain",
      fileName: undefined,
      chunks: [],
      totalSizeBytes: undefined,
      finished: false,
    });

    stream.push(new Uint8Array([1, 2, 3]));

    const update3 = (await queue.next()).value;
    expect(update3.getChunks({ allowUnfinished: true })).toEqual({
      mimeType: "text/plain",
      fileName: undefined,
      chunks: [new Uint8Array([1, 2, 3])],
      totalSizeBytes: undefined,
      finished: false,
    });

    stream.push(new Uint8Array([4, 5, 6]));

    const update4 = (await queue.next()).value;
    expect(update4.getChunks({ allowUnfinished: true })).toEqual({
      mimeType: "text/plain",
      fileName: undefined,
      chunks: [new Uint8Array([1, 2, 3]), new Uint8Array([4, 5, 6])],
      totalSizeBytes: undefined,
      finished: false,
    });

    stream.end();

    const update5 = (await queue.next()).value;
    expect(update5.getChunks()).toEqual({
      mimeType: "text/plain",
      fileName: undefined,
      chunks: [new Uint8Array([1, 2, 3]), new Uint8Array([4, 5, 6])],
      totalSizeBytes: undefined,
      finished: true,
    });
  });

  test("Subscription without options", async () => {
    const { me } = await initNodeAndStream();
    const group = Group.create(me);
    group.makePublic();
    const stream = FileStream.create({ owner: group });

    const queue = new Channel();

    FileStream.subscribe(stream.$jazz.id, (subscribedStream) => {
      void queue.push(subscribedStream);
    });

    // Initial state
    const update1 = (await queue.next()).value;
    expect(update1.getChunks()).toBe(undefined);

    // Start the stream
    stream.start({ mimeType: "text/plain" });
    const update2 = (await queue.next()).value;
    expect(update2.getChunks({ allowUnfinished: true })).toMatchObject({
      mimeType: "text/plain",
      finished: false,
    });

    // Push a chunk
    stream.push(new Uint8Array([1, 2, 3]));
    const update3 = (await queue.next()).value;
    expect(update3.getChunks({ allowUnfinished: true })?.chunks).toHaveLength(
      1,
    );
    expect(update3.getChunks({ allowUnfinished: true })?.chunks?.[0]).toEqual(
      new Uint8Array([1, 2, 3]),
    );

    // End the stream
    stream.end();
    const update4 = (await queue.next()).value;
    expect(update4.getChunks()?.finished).toBe(true);
  });
});

describe("FileStream.load", async () => {
  async function setup() {
    const me = await createJazzTestAccount({
      isCurrentActiveAccount: true,
    });

    const group = Group.create(me);
    group.makePublic();
    const stream = FileStream.create({ owner: group });

    stream.start({ mimeType: "text/plain" });

    return { stream, me };
  }

  test("resolves only when the stream is ended", async () => {
    const { stream, me } = await setup();
    stream.push(new Uint8Array([1]));

    const promise = FileStream.load(stream.$jazz.id, { loadAs: me });

    stream.push(new Uint8Array([2]));
    stream.end();

    const blob = await promise;

    // The promise resolves only when the stream is ended
    // so we get a blob with all the chunks
    expect(blob?.getChunks()?.finished).toBe(true);
  });

  test("resolves with an unfinshed blob if allowUnfinished: true", async () => {
    const { stream, me } = await setup();
    stream.push(new Uint8Array([1]));

    const promise = FileStream.load(stream.$jazz.id, {
      loadAs: me,
      allowUnfinished: true,
    });

    const blob = await promise;

    stream.push(new Uint8Array([2]));

    // The promise resolves before the stream is ended
    // so we get a blob only with the first chunk
    expect(blob?.getChunks({ allowUnfinished: true })?.finished).toBe(false);
  });
});

describe("FileStream.loadAsBlob", async () => {
  async function setup() {
    const me = await Account.create({
      creationProps: { name: "Hermes Puggington" },
      crypto: Crypto,
    });

    const stream = FileStream.create({ owner: me });

    stream.start({ mimeType: "text/plain" });

    return { stream, me };
  }

  test("resolves only when the stream is ended", async () => {
    const { stream, me } = await setup();
    stream.push(new Uint8Array([1]));

    const promise = FileStream.loadAsBlob(stream.$jazz.id, { loadAs: me });

    stream.push(new Uint8Array([2]));
    stream.end();

    const blob = await promise;

    // The promise resolves only when the stream is ended
    // so we get a blob with all the chunks
    expect(blob?.size).toBe(2);
  });

  test("resolves with an unfinshed blob if allowUnfinished: true", async () => {
    const { stream, me } = await setup();
    stream.push(new Uint8Array([1]));

    const promise = FileStream.loadAsBlob(stream.$jazz.id, {
      loadAs: me,
      allowUnfinished: true,
    });

    const blob = await promise;

    stream.push(new Uint8Array([2]));
    stream.end();

    // The promise resolves before the stream is ended
    // so we get a blob only with the first chunk
    expect(blob?.size).toBe(1);
  });
});

describe("FileStream.loadAsBase64", async () => {
  async function setup() {
    const me = await Account.create({
      creationProps: { name: "Hermes Puggington" },
      crypto: Crypto,
    });

    const stream = FileStream.create({ owner: me });

    stream.start({ mimeType: "text/plain" });

    return { stream, me };
  }

  test("resolves only when the stream is ended", async () => {
    const { stream, me } = await setup();
    stream.push(new Uint8Array([1]));

    const promise = FileStream.loadAsBase64(stream.$jazz.id, { loadAs: me });

    stream.push(new Uint8Array([2]));
    stream.end();

    const base64 = await promise;

    // The promise resolves only when the stream is ended
    // so we get a blob with all the chunks
    expect(base64).toBe("AQI=");
  });

  test("resolves with a data URL if dataURL: true", async () => {
    const { stream, me } = await setup();
    stream.push(new Uint8Array([1]));

    const promise = FileStream.loadAsBase64(stream.$jazz.id, {
      loadAs: me,
      dataURL: true,
    });

    stream.push(new Uint8Array([2]));
    stream.end();

    const base64 = await promise;

    // The promise resolves only when the stream is ended
    // so we get a blob with all the chunks
    expect(base64).toBe("data:text/plain;base64,AQI=");
  });

  test("resolves with a partial base64 if allowUnfinished: true", async () => {
    const { stream, me } = await setup();
    stream.push(new Uint8Array([1]));

    const promise = FileStream.loadAsBase64(stream.$jazz.id, {
      loadAs: me,
      allowUnfinished: true,
    });

    const base64 = await promise;

    stream.push(new Uint8Array([2]));
    stream.end();

    // The promise resolves before the stream is ended
    // so we get a blob only with the first chunk
    expect(base64).toBe("AQ==");
  });
});

describe("FileStream progress tracking", async () => {
  test("createFromBlob should report upload progress correctly", async () => {
    // Create 5MB test blob
    const testData = new Uint8Array(5 * 1024 * 1024); // 5MB instead of 500KB
    for (let i = 0; i < testData.length; i++) testData[i] = i % 256;
    const testBlob = new Blob([testData]);

    // Collect progress updates
    const progressUpdates: number[] = [];
    await FileStream.createFromBlob(testBlob, {
      onProgress: (progress) => progressUpdates.push(progress),
    });

    // Verify progress reporting
    expect(progressUpdates.length).toBeGreaterThan(1);

    // Check values between 0-1, increasing, with final=1
    progressUpdates.forEach((p) => {
      expect(p).toBeGreaterThanOrEqual(0);
      expect(p).toBeLessThanOrEqual(1);
    });

    for (let i = 1; i < progressUpdates.length; i++) {
      expect(progressUpdates[i]!).toBeGreaterThanOrEqual(
        progressUpdates[i - 1]!,
      );
    }

    expect(progressUpdates[progressUpdates.length - 1]).toBe(1);
  });
});

describe("FileStream large file loading", async () => {
  test("load a large FileStream with allowUnfinished: true should return the loaded file before it's fully loaded", async () => {
    const syncServer = await setupJazzTestSync({ asyncPeers: true });

    const group = Group.create(syncServer);
    const largeStream = FileStream.create({ owner: group });
    group.addMember("everyone", "reader");

    // Create a large file stream with multiple chunks
    largeStream.start({ mimeType: "application/octet-stream" });

    const dataSize = 100 * 1024; // 100KB total
    const chunkSize = 1024; // 1KB chunks
    const numChunks = dataSize / chunkSize;

    // Create test data chunks
    for (let i = 0; i < numChunks; i++) {
      const chunk = new Uint8Array(chunkSize);
      for (let j = 0; j < chunkSize; j++) {
        chunk[j] = (i * chunkSize + j) % 256;
      }
      largeStream.push(chunk);
    }

    largeStream.end();

    // Wait for the large FileStream to be fully synced
    await largeStream.$jazz.waitForSync();

    const alice = await createJazzTestAccount();

    // Test loading the large FileStream
    const loadedStream = await FileStream.load(largeStream.$jazz.id, {
      loadAs: alice,
      allowUnfinished: true,
    });

    assert(loadedStream);

    const loadedChunks = loadedStream.getChunks({ allowUnfinished: true });
    expect(loadedChunks).not.toBeNull();
    expect(loadedChunks?.finished).toBe(undefined);

    expect(loadedStream.$jazz.raw.core.knownState()).not.toEqual(
      largeStream.$jazz.raw.core.knownState(),
    );
  });

  test("load a large FileStream with allowUnfinished: false should return the loaded file only when it's fully loaded", async () => {
    const syncServer = await setupJazzTestSync({ asyncPeers: true });

    const group = Group.create(syncServer);
    const largeStream = FileStream.create({ owner: group });
    group.addMember("everyone", "reader");

    // Create a large file stream with multiple chunks
    largeStream.start({ mimeType: "application/octet-stream" });

    const dataSize = 100 * 1024; // 100KB total
    const chunkSize = 1024; // 1KB chunks
    const numChunks = dataSize / chunkSize;

    // Create test data chunks
    for (let i = 0; i < numChunks; i++) {
      const chunk = new Uint8Array(chunkSize);
      for (let j = 0; j < chunkSize; j++) {
        chunk[j] = (i * chunkSize + j) % 256;
      }
      largeStream.push(chunk);
    }

    largeStream.end();

    // Wait for the large FileStream to be fully synced
    await largeStream.$jazz.waitForSync();

    const alice = await createJazzTestAccount();

    // Test loading the large FileStream
    const loadedStream = await FileStream.load(largeStream.$jazz.id, {
      loadAs: alice,
      allowUnfinished: false,
    });

    assert(loadedStream);

    const loadedChunks = loadedStream.getChunks();
    expect(loadedChunks).not.toBeNull();
    expect(loadedChunks?.finished).toBe(true);
    expect(loadedChunks?.chunks).toHaveLength(numChunks); // 100 chunks of 1KB each

    expect(loadedStream.$jazz.raw.core.knownState()).toEqual(
      largeStream.$jazz.raw.core.knownState(),
    );
  });
});

describe("waitForSync", async () => {
  test("CoFeed: should resolve when the value is uploaded", async () => {
    const TestStream = co.feed(z.string());

    const { clientNode, serverNode, clientAccount } = await setupTwoNodes();

    const stream = TestStream.create(["1", "2", "3"], { owner: clientAccount });

    await stream.$jazz.waitForSync({ timeout: 1000 });

    // Killing the client node so the serverNode can't load the map from it
    clientNode.gracefulShutdown();

    const loadedStream = await serverNode.load(stream.$jazz.raw.id);

    expect(loadedStream).not.toBe("unavailable");
  });

  test("FileStream: should resolve when the value is uploaded", async () => {
    const { clientNode, serverNode, clientAccount } = await setupTwoNodes();

    const stream = FileStream.create({ owner: clientAccount });

    stream.start({ mimeType: "text/plain" });
    stream.push(new Uint8Array([2]));
    stream.end();

    await stream.$jazz.waitForSync({ timeout: 1000 });

    // Killing the client node so the serverNode can't load the map from it
    clientNode.gracefulShutdown();

    const loadedStream = await serverNode.load(stream.$jazz.raw.id);

    expect(loadedStream).not.toBe("unavailable");
  });

  test("should rely on the current active account if no account is provided", async () => {
    const account = await createJazzTestAccount({
      isCurrentActiveAccount: true,
    });

    const stream = FileStream.create();
    expect(stream.$jazz.owner[TypeSym]).toEqual("Group");
    expect(stream.$jazz.owner.$jazz.raw.roleOf(account.$jazz.raw.id)).toEqual(
      "admin",
    );
  });
});
