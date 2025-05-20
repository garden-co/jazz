import { WasmCrypto } from "cojson/crypto/WasmCrypto";
import { describe, expect, expectTypeOf, test } from "vitest";
import {
  Account,
  FileStream,
  Group,
  co,
  cojsonInternals,
  isControlledAccount,
  z,
} from "../index.js";
import {
  Loaded,
  createJazzContextFromExistingCredentials,
  randomSessionProvider,
} from "../internal.js";
import { createJazzTestAccount } from "../testing.js";
import { setupTwoNodes } from "./utils.js";

const Crypto = await WasmCrypto.create();

const connectedPeers = cojsonInternals.connectedPeers;

describe("Simple CoFeed operations", async () => {
  const me = await Account.create({
    creationProps: { name: "Hermes Puggington" },
    crypto: Crypto,
  });
  if (!isControlledAccount(me)) {
    throw "me is not a controlled account";
  }
  const TestStream = co.feed(z.string());

  const stream = TestStream.create(["milk"], { owner: me });

  test("Construction", () => {
    expect(stream.perAccount[me.id]?.value).toEqual("milk");
    expect(stream.perSession[me.sessionID]?.value).toEqual("milk");
  });

  test("Construction with an Account", () => {
    const stream = TestStream.create(["milk"], me);

    expect(stream.perAccount[me.id]?.value).toEqual("milk");
    expect(stream.perSession[me.sessionID]?.value).toEqual("milk");
  });

  test("Construction with a Group", () => {
    const group = Group.create(me);
    const stream = TestStream.create(["milk"], group);

    expect(stream.perAccount[me.id]?.value).toEqual("milk");
    expect(stream.perSession[me.sessionID]?.value).toEqual("milk");
  });

  describe("Mutation", () => {
    test("pushing", () => {
      stream.push("bread");
      expect(stream.perAccount[me.id]?.value).toEqual("bread");
      expect(stream.perSession[me.sessionID]?.value).toEqual("bread");

      stream.push("butter");
      expect(stream.perAccount[me.id]?.value).toEqual("butter");
      expect(stream.perSession[me.sessionID]?.value).toEqual("butter");
    });
  });
});

describe("CoFeed resolution", async () => {
  const TwiceNestedStream = co.feed(z.string());
  const NestedStream = co.feed(TwiceNestedStream);
  const TestStream = co.feed(NestedStream);

  const initNodeAndStream = async () => {
    const me = await Account.create({
      creationProps: { name: "Hermes Puggington" },
      crypto: Crypto,
    });

    const stream = TestStream.create(
      [
        NestedStream.create(
          [TwiceNestedStream.create(["milk"], { owner: me })],
          { owner: me },
        ),
      ],
      { owner: me },
    );

    return { me, stream };
  };

  test("Construction", async () => {
    const { me, stream } = await initNodeAndStream();

    // TODO: fix this
    // expectTypeOf(stream[me.id]).not.toBeAny();

    expect(
      stream.perAccount[me.id]?.value?.perAccount[me.id]?.value?.perAccount[
        me.id
      ]?.value,
    ).toEqual("milk");
  });

  test("Loading and availability", async () => {
    const { me, stream } = await initNodeAndStream();
    const [initialAsPeer, secondPeer] = connectedPeers("initial", "second", {
      peer1role: "server",
      peer2role: "client",
    });
    if (!isControlledAccount(me)) {
      throw "me is not a controlled account";
    }
    me._raw.core.node.syncManager.addPeer(secondPeer);
    const { account: meOnSecondPeer } =
      await createJazzContextFromExistingCredentials({
        credentials: {
          accountID: me.id,
          secret: me._raw.core.node.getCurrentAgent().agentSecret,
        },
        sessionProvider: randomSessionProvider,
        peersToLoadFrom: [initialAsPeer],
        crypto: Crypto,
      });

    const loadedStream = await TestStream.load(stream.id, {
      loadAs: meOnSecondPeer,
    });

    // TODO: fix this
    // expectTypeOf(loadedStream?.[me.id]).not.toBeAny();

    expect(loadedStream?.perAccount[me.id]?.value).toEqual(null);
    expect(loadedStream?.perAccount[me.id]?.ref?.id).toEqual(
      stream.perAccount[me.id]?.value?.id,
    );

    const loadedNestedStream = await NestedStream.load(
      stream.perAccount[me.id]!.value!.id,
      { loadAs: meOnSecondPeer },
    );

    // expect(loadedStream?.[me.id]?.value).toEqual(loadedNestedStream);
    expect(loadedStream?.perAccount[me.id]?.value?.id).toEqual(
      loadedNestedStream?.id,
    );
    expect(
      loadedStream?.perAccount[me.id]?.value?.perAccount[me.id]?.value,
    ).toEqual(null);
    // expect(loadedStream?.[me.id]?.ref?.value).toEqual(loadedNestedStream);
    expect(loadedStream?.perAccount[me.id]?.ref?.value?.id).toEqual(
      loadedNestedStream?.id,
    );
    expect(
      loadedStream?.perAccount[me.id]?.value?.perAccount[me.id]?.ref?.id,
    ).toEqual(stream.perAccount[me.id]?.value?.perAccount[me.id]?.value?.id);

    const loadedTwiceNestedStream = await TwiceNestedStream.load(
      stream.perAccount[me.id]!.value!.perAccount[me.id]!.value!.id,
      { loadAs: meOnSecondPeer },
    );

    // expect(loadedStream?.[me.id]?.value?.[me.id]?.value).toEqual(
    //     loadedTwiceNestedStream
    // );
    expect(
      loadedStream?.perAccount[me.id]?.value?.perAccount[me.id]?.value?.id,
    ).toEqual(loadedTwiceNestedStream?.id);
    // expect(loadedStream?.[me.id]?.ref?.value).toEqual(loadedNestedStream);
    expect(loadedStream?.perAccount[me.id]?.ref?.value?.id).toEqual(
      loadedNestedStream?.id,
    );
    expect(
      loadedStream?.perAccount[me.id]?.value?.perAccount[me.id]?.ref?.value?.id,
    ).toEqual(loadedTwiceNestedStream?.id);

    const otherNestedStream = NestedStream.create(
      [TwiceNestedStream.create(["butter"], { owner: meOnSecondPeer })],
      { owner: meOnSecondPeer },
    );
    loadedStream?.push(otherNestedStream);
    // expect(loadedStream?.[me.id]?.value).toEqual(otherNestedStream);
    expect(loadedStream?.perAccount[me.id]?.value?.id).toEqual(
      otherNestedStream?.id,
    );
    expect(loadedStream?.perAccount[me.id]?.ref?.value?.id).toEqual(
      otherNestedStream?.id,
    );
    expect(
      loadedStream?.perAccount[me.id]?.value?.perAccount[me.id]?.value?.id,
    ).toEqual(otherNestedStream.perAccount[me.id]?.value?.id);
  });

  test("Subscription & auto-resolution", async () => {
    const { me, stream } = await initNodeAndStream();

    const [initialAsPeer, secondAsPeer] = connectedPeers("initial", "second", {
      peer1role: "server",
      peer2role: "client",
    });

    me._raw.core.node.syncManager.addPeer(secondAsPeer);
    if (!isControlledAccount(me)) {
      throw "me is not a controlled account";
    }
    const { account: meOnSecondPeer } =
      await createJazzContextFromExistingCredentials({
        credentials: {
          accountID: me.id,
          secret: me._raw.core.node.getCurrentAgent().agentSecret,
        },
        sessionProvider: randomSessionProvider,
        peersToLoadFrom: [initialAsPeer],
        crypto: Crypto,
      });

    const queue = new cojsonInternals.Channel();

    TestStream.subscribe(
      stream.id,
      { loadAs: meOnSecondPeer },
      (subscribedStream) => {
        void queue.push(subscribedStream);
      },
    );

    const update1 = (await queue.next()).value;
    expect(update1.perAccount[me.id]?.value).toEqual(null);

    const update2 = (await queue.next()).value;
    expect(update2.perAccount[me.id]?.value).toBeDefined();
    expect(update2.perAccount[me.id]?.value?.perAccount[me.id]?.value).toBe(
      null,
    );

    const update3 = (await queue.next()).value;
    expect(
      update3.perAccount[me.id]?.value?.perAccount[me.id]?.value,
    ).toBeDefined();
    expect(
      update3.perAccount[me.id]?.value?.perAccount[me.id]?.value?.perAccount[
        me.id
      ]?.value,
    ).toBe("milk");

    update3.perAccount[me.id]!.value!.perAccount[me.id]!.value!.push("bread");

    const update4 = (await queue.next()).value;
    expect(
      update4.perAccount[me.id]?.value?.perAccount[me.id]?.value?.perAccount[
        me.id
      ]?.value,
    ).toBe("bread");

    // When assigning a new nested stream, we get an update
    const newTwiceNested = TwiceNestedStream.create(["butter"], {
      owner: meOnSecondPeer,
    });

    const newNested = NestedStream.create([newTwiceNested], {
      owner: meOnSecondPeer,
    });

    update4.push(newNested);

    const update5 = (await queue.next()).value;
    expect(
      update5.perAccount[me.id]?.value?.perAccount[me.id]?.value?.perAccount[
        me.id
      ]?.value,
    ).toBe("butter");

    // we get updates when the new nested stream changes
    newTwiceNested.push("jam");
    const update6 = (await queue.next()).value;
    expect(
      update6.perAccount[me.id]?.value?.perAccount[me.id]?.value?.perAccount[
        me.id
      ]?.value,
    ).toBe("jam");
  });

  test("Subscription without options", async () => {
    const { me, stream } = await initNodeAndStream();

    const [initialAsPeer, secondAsPeer] = connectedPeers("initial", "second", {
      peer1role: "server",
      peer2role: "client",
    });
    if (!isControlledAccount(me)) {
      throw "me is not a controlled account";
    }
    me._raw.core.node.syncManager.addPeer(secondAsPeer);

    await createJazzContextFromExistingCredentials({
      credentials: {
        accountID: me.id,
        secret: me._raw.core.node.getCurrentAgent().agentSecret,
      },
      sessionProvider: randomSessionProvider,
      peersToLoadFrom: [initialAsPeer],
      crypto: Crypto,
    });

    const queue = new cojsonInternals.Channel<Loaded<typeof TestStream>>();

    TestStream.subscribe(stream.id, (subscribedStream) => {
      void queue.push(subscribedStream);
    });

    const update1 = (await queue.next()).value;
    expect(update1.perAccount[me.id]?.value).toEqual(null);

    const update2 = (await queue.next()).value;
    expect(update2.perAccount[me.id]?.value?.perAccount[me.id]?.value).toEqual(
      null,
    );

    const update3 = (await queue.next()).value;
    expect(
      update3.perAccount[me.id]?.value?.perAccount[me.id]?.value,
    ).toBeDefined();
    expect(
      update3.perAccount[me.id]?.value?.perAccount[me.id]?.value?.perAccount[
        me.id
      ]?.value,
    ).toBe("milk");

    update3.perAccount[me.id]!.value!.perAccount[me.id]!.value!.push("bread");

    const update4 = (await queue.next()).value;
    expect(
      update4.perAccount[me.id]?.value?.perAccount[me.id]?.value?.perAccount[
        me.id
      ]?.value,
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
    const me = await Account.create({
      creationProps: { name: "Hermes Puggington" },
      crypto: Crypto,
    });

    const stream = FileStream.create({ owner: me });

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
    const { me, stream } = await initNodeAndStream();
    const [initialAsPeer, secondAsPeer] = connectedPeers("initial", "second", {
      peer1role: "server",
      peer2role: "client",
    });
    if (!isControlledAccount(me)) {
      throw "me is not a controlled account";
    }
    me._raw.core.node.syncManager.addPeer(secondAsPeer);
    const { account: meOnSecondPeer } =
      await createJazzContextFromExistingCredentials({
        credentials: {
          accountID: me.id,
          secret: me._raw.core.node.getCurrentAgent().agentSecret,
        },
        sessionProvider: randomSessionProvider,
        peersToLoadFrom: [initialAsPeer],
        crypto: Crypto,
      });

    const loadedStream = await FileStream.load(stream.id, {
      loadAs: meOnSecondPeer,
    });

    expect(loadedStream?.getChunks()).toEqual({
      mimeType: "text/plain",
      chunks: [new Uint8Array([1, 2, 3]), new Uint8Array([4, 5, 6])],
      finished: true,
    });
  });

  test("Subscription", async () => {
    const { me } = await initNodeAndStream();
    const stream = FileStream.create({ owner: me });

    const [initialAsPeer, secondAsPeer] = connectedPeers("initial", "second", {
      peer1role: "server",
      peer2role: "client",
    });
    me._raw.core.node.syncManager.addPeer(secondAsPeer);
    if (!isControlledAccount(me)) {
      throw "me is not a controlled account";
    }
    const { account: meOnSecondPeer } =
      await createJazzContextFromExistingCredentials({
        credentials: {
          accountID: me.id,
          secret: me._raw.core.node.getCurrentAgent().agentSecret,
        },
        sessionProvider: randomSessionProvider,
        peersToLoadFrom: [initialAsPeer],
        crypto: Crypto,
      });

    const queue = new cojsonInternals.Channel();

    FileStream.subscribe(
      stream.id,
      { loadAs: meOnSecondPeer },
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
    const stream = FileStream.create({ owner: me });

    const [initialAsPeer, secondAsPeer] = connectedPeers("initial", "second", {
      peer1role: "server",
      peer2role: "client",
    });
    me._raw.core.node.syncManager.addPeer(secondAsPeer);
    if (!isControlledAccount(me)) {
      throw "me is not a controlled account";
    }
    await createJazzContextFromExistingCredentials({
      credentials: {
        accountID: me.id,
        secret: me._raw.core.node.getCurrentAgent().agentSecret,
      },
      sessionProvider: randomSessionProvider,
      peersToLoadFrom: [initialAsPeer],
      crypto: Crypto,
    });

    const queue = new cojsonInternals.Channel();

    FileStream.subscribe(stream.id, (subscribedStream) => {
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

    const promise = FileStream.loadAsBlob(stream.id, { loadAs: me });

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

    const promise = FileStream.loadAsBlob(stream.id, {
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

describe("waitForSync", async () => {
  test("CoFeed: should resolve when the value is uploaded", async () => {
    const TestStream = co.feed(z.string());

    const { clientNode, serverNode, clientAccount } = await setupTwoNodes();

    const stream = TestStream.create(["1", "2", "3"], { owner: clientAccount });

    await stream.waitForSync({ timeout: 1000 });

    // Killing the client node so the serverNode can't load the map from it
    clientNode.gracefulShutdown();

    const loadedStream = await serverNode.load(stream._raw.id);

    expect(loadedStream).not.toBe("unavailable");
  });

  test("FileStream: should resolve when the value is uploaded", async () => {
    const { clientNode, serverNode, clientAccount } = await setupTwoNodes();

    const stream = FileStream.create({ owner: clientAccount });

    stream.start({ mimeType: "text/plain" });
    stream.push(new Uint8Array([2]));
    stream.end();

    await stream.waitForSync({ timeout: 1000 });

    // Killing the client node so the serverNode can't load the map from it
    clientNode.gracefulShutdown();

    const loadedStream = await serverNode.load(stream._raw.id);

    expect(loadedStream).not.toBe("unavailable");
  });

  test("should rely on the current active account if no account is provided", async () => {
    const account = await createJazzTestAccount({
      isCurrentActiveAccount: true,
    });

    const stream = FileStream.create();
    expect(stream._owner._type).toEqual("Group");
    expect(stream._owner.castAs(Group)._raw.roleOf(account._raw.id)).toEqual(
      "admin",
    );
  });
});
