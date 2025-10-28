import { WasmCrypto } from "cojson/crypto/WasmCrypto";
import { Channel } from "queueueue";
import { describe, expect, test, vi } from "vitest";
import {
  Account,
  cojsonInternals,
  createJazzContextFromExistingCredentials,
  isControlledAccount,
} from "../index.js";
import { co, randomSessionProvider } from "../internal.js";

const Crypto = await WasmCrypto.create();

const connectedPeers = cojsonInternals.connectedPeers;

describe("CoPlainText", () => {
  const initNodeAndText = async () => {
    const me = await Account.create({
      creationProps: { name: "Hermes Puggington" },
      crypto: Crypto,
    });

    const text = co.plainText().create("hello world", { owner: me });

    return { me, text };
  };

  describe("Creation", () => {
    test("should allow `create`", async () => {
      const me = await Account.create({
        creationProps: { name: "Hermes Puggington" },
        crypto: Crypto,
      });
      const text = co.plainText().create("hello world", me);
      expect(text.$jazz.owner.$jazz.id).toBe(me.$jazz.id);
    });

    test("should allow `create` from raw", async () => {
      const me = await Account.create({
        creationProps: { name: "Hermes Puggington" },
        crypto: Crypto,
      });
      const text = co.plainText().create("hello world", me);
      const raw = text.$jazz.raw;
      const text2 = co.plainText().fromRaw(raw);
      expect(text2.$jazz.owner.$jazz.id).toBe(me.$jazz.id);
    });

    test("should allow owner shorthand", async () => {
      const me = await Account.create({
        creationProps: { name: "Hermes Puggington" },
        crypto: Crypto,
      });
      const text = co.plainText().create("hello world", me);
      expect(text.$jazz.owner.$jazz.id).toBe(me.$jazz.id);
    });
  });

  describe("Simple CoPlainText operations", async () => {
    const { me, text } = await initNodeAndText();

    test("Construction", () => {
      expect(text + "").toEqual("hello world");
    });

    describe("Mutation", () => {
      test("insertion", () => {
        const text = co.plainText().create("hello world", { owner: me });

        text.insertAfter(4, " cruel");
        expect(text + "").toEqual("hello cruel world");

        text.insertBefore(0, "Hello, ");
        expect(text + "").toEqual("Hello, hello cruel world");
      });

      test("deletion", () => {
        const text = co.plainText().create("hello world", { owner: me });

        text.deleteRange({ from: 3, to: 8 });
        expect(text + "").toEqual("helrld");
      });

      test("applyDiff", () => {
        const text = co.plainText().create("hello world", { owner: me });
        text.$jazz.applyDiff("hello cruel world");
        expect(text.toString()).toEqual("hello cruel world");
      });

      test("applyDiff with complex grapheme clusters", () => {
        const text = co.plainText().create(`😊`, { owner: me });
        text.$jazz.applyDiff(`😊안녕!`);
        expect(text.toString()).toEqual(`😊안녕!`);
        text.$jazz.applyDiff(`😊👋 안녕!`);
        expect(text.toString()).toEqual(`😊👋 안녕!`);
      });

      test("applyDiff should emit a single update", () => {
        const Text = co.plainText();

        const text = Text.create(`😊`, { owner: me });

        const updateFn = vi.fn();

        const unsubscribe = Text.subscribe(
          text.$jazz.id,
          {
            loadAs: me,
          },
          updateFn,
        );

        updateFn.mockClear();

        text.$jazz.applyDiff(`😊👋 안녕!`);

        expect(updateFn).toHaveBeenCalledTimes(1);

        unsubscribe();
      });
    });

    describe("Properties", () => {
      test("length", () => {
        const text = co.plainText().create("hello world", { owner: me });
        expect(text.length).toBe(11);
      });

      test("as string", () => {
        const text = co.plainText().create("hello world", { owner: me });
        expect(`${text}`).toBe("hello world");
      });

      test("as number", () => {
        const text = co.plainText().create("hello world", { owner: me });
        expect(Number(text)).toBe(NaN);
      });

      test("as number", () => {
        const text = co.plainText().create("123", { owner: me });
        expect(Number(text)).toBe(123);
      });

      test("toJSON", () => {
        const text = co.plainText().create("hello world", { owner: me });
        expect(text.toJSON()).toBe("hello world");
      });

      test("toString", () => {
        const text = co.plainText().create("hello world", { owner: me });
        expect(text.toString()).toBe("hello world");
      });
    });

    describe("Position operations", () => {
      test("idxBefore returns index before a position", () => {
        const text = co.plainText().create("hello world", { owner: me });

        // Get position at index 5 (between "hello" and " world")
        const pos = text.posBefore(5);
        expect(pos).toBeDefined();

        // Verify idxBefore returns the index before the position (4)
        // This makes sense as the position is between characters,
        // and idxBefore returns the index of the last character before that position
        const idx = text.idxBefore(pos!);
        expect(idx).toBe(4); // Index of 'o' in "hello"
      });

      test("idxAfter returns index after a position", () => {
        const text = co.plainText().create("hello world", { owner: me });

        // Get position at index 5 (between "hello" and " world")
        const pos = text.posBefore(5);
        expect(pos).toBeDefined();

        // Verify idxAfter returns the index after the position (5)
        // This makes sense as the position is between characters,
        // and idxAfter returns the index of the first character after that position
        const idx = text.idxAfter(pos!);
        expect(idx).toBe(5); // Index of ' ' in "hello world"
      });
    });
  });

  describe("Loading and availability", () => {
    test("can load text across peers", async () => {
      const { me, text } = await initNodeAndText();
      const id = text.$jazz.id;

      // Set up peer connections
      const [initialAsPeer, secondPeer] = connectedPeers("initial", "second", {
        peer1role: "server",
        peer2role: "client",
      });

      if (!isControlledAccount(me)) {
        throw "me is not a controlled account";
      }
      me.$jazz.localNode.syncManager.addPeer(secondPeer);
      const { account: meOnSecondPeer } =
        await createJazzContextFromExistingCredentials({
          credentials: {
            accountID: me.$jazz.id,
            secret: me.$jazz.localNode.getCurrentAgent().agentSecret,
          },
          sessionProvider: randomSessionProvider,
          peers: [initialAsPeer],
          crypto: Crypto,
          asActiveAccount: true,
        });

      // Load the text on the second peer
      const loaded = await co.plainText().load(id, { loadAs: meOnSecondPeer });
      expect(loaded).toBeDefined();
      expect(loaded!.toString()).toBe("hello world");
    });
  });

  test("Subscription & auto-resolution", async () => {
    const { me, text } = await initNodeAndText();

    // Set up peer connections
    const [initialAsPeer, secondPeer] = connectedPeers("initial", "second", {
      peer1role: "server",
      peer2role: "client",
    });

    if (!isControlledAccount(me)) {
      throw "me is not a controlled account";
    }
    me.$jazz.localNode.syncManager.addPeer(secondPeer);
    const { account: meOnSecondPeer } =
      await createJazzContextFromExistingCredentials({
        credentials: {
          accountID: me.$jazz.id,
          secret: me.$jazz.localNode.getCurrentAgent().agentSecret,
        },
        sessionProvider: randomSessionProvider,
        peers: [initialAsPeer],
        crypto: Crypto,
        asActiveAccount: true,
      });

    const queue = new Channel();

    // Subscribe to text updates
    co.plainText().subscribe(
      text.$jazz.id,
      { loadAs: meOnSecondPeer },
      (subscribedText) => {
        void queue.push(subscribedText);
      },
    );

    // Initial subscription should give us the text
    const update1 = (await queue.next()).value;
    expect(update1.toString()).toBe("hello world");

    // When we make a change, we should get an update
    text.insertAfter(4, " beautiful");
    const update2 = (await queue.next()).value;
    expect(update2.toString()).toBe("hello beautiful world");

    // When we make another change, we should get another update
    update2.deleteRange({ from: 5, to: 15 }); // Delete " beautiful"
    const update3 = (await queue.next()).value;
    expect(update3.toString()).toBe("hello world");
  });
});

describe("lastUpdatedAt", () => {
  test("empty text last updated time", () => {
    const text = co.plainText().create("");

    expect(text.$jazz.lastUpdatedAt).toEqual(text.$jazz.createdAt);
    expect(text.$jazz.lastUpdatedAt).not.toEqual(0);
  });

  test("last update should change on push", async () => {
    const text = co.plainText().create("John");

    expect(text.$jazz.lastUpdatedAt).not.toEqual(0);

    const updatedAt = text.$jazz.lastUpdatedAt;

    await new Promise((r) => setTimeout(r, 10));
    text.$jazz.applyDiff("Jane");

    expect(text.$jazz.lastUpdatedAt).not.toEqual(updatedAt);
  });
});
