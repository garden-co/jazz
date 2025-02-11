import { connectedPeers } from "cojson/src/streamUtils.ts";
import { describe, expect, test } from "vitest";
import { CoRichTextDebug, splitNode } from "../coValues/coRichText.js";
import {
  Account,
  CoRichText,
  Marks,
  type TextPos,
  type TreeNode,
  WasmCrypto,
  cojsonInternals,
  createJazzContext,
  fixedCredentialsAuth,
  isControlledAccount,
} from "../index.web.js";
import { randomSessionProvider } from "../internal.js";

const Crypto = await WasmCrypto.create();

describe("CoRichText", async () => {
  const me = await Account.create({
    creationProps: { name: "Hermes Puggington" },
    crypto: Crypto,
  });

  const text = CoRichText.createFromPlainText("hello world", { owner: me });

  test("Construction", () => {
    expect(text + "").toEqual("hello world");
  });

  describe("Mutation", () => {
    test("insertion", () => {
      const text = CoRichText.createFromPlainText("hello world", {
        owner: me,
      });

      text.insertAfter(4, " cruel");
      expect(text + "").toEqual("hello cruel world");
    });

    test("deletion", () => {
      const text = CoRichText.createFromPlainText("hello world", {
        owner: me,
      });

      text.deleteRange({ from: 3, to: 8 });
      expect(text + "").toEqual("helrld");
    });

    describe("inserting marks", () => {
      test("basic mark insertion", () => {
        const text = CoRichText.createFromPlainText("hello world", {
          owner: me,
        });

        // Add mark covering "hello"
        text.insertMark(0, 4, Marks.Strong, { tag: "strong" });

        const marks = text.resolveMarks();
        expect(marks).toHaveLength(1);
        expect(marks[0]).toMatchObject({
          startAfter: -1,
          startBefore: 0,
          endAfter: 4,
          endBefore: 5,
          tag: "strong",
        });
      });

      test("inserting mark with custom owner", () => {
        const text = CoRichText.createFromPlainText("hello world", {
          owner: me,
        });

        text.insertMark(
          0,
          5,
          Marks.Strong,
          { tag: "strong" },
          { markOwner: me },
        );

        const mark = text.marks![0];
        expect(mark!._owner).toStrictEqual(me);
      });

      test("inserting multiple non-overlapping marks", () => {
        const text = CoRichText.createFromPlainText("hello world", {
          owner: me,
        });

        text.insertMark(0, 4, Marks.Strong, { tag: "strong" }); // "hello"
        text.insertMark(6, 10, Marks.Em, { tag: "em" }); // "world"

        const marks = text.resolveMarks();
        expect(marks).toHaveLength(2);

        // Verify positions and types
        const [mark1, mark2] = marks;
        expect(mark1!.sourceMark.tag).toBe("strong");
        expect(mark2!.sourceMark.tag).toBe("em");

        expect(mark1!.startAfter).toBe(-1);
        expect(mark1!.startBefore).toBe(0);
        expect(mark1!.endAfter).toBe(4);
        expect(mark1!.endBefore).toBe(5);
        expect(mark2!.startAfter).toBe(5);
        expect(mark2!.startBefore).toBe(6);
        expect(mark2!.endAfter).toBe(10);
        expect(mark2!.endBefore).toBe(11);
      });

      test("inserting mark with additional properties", () => {
        const text = CoRichText.createFromPlainText("hello world", {
          owner: me,
        });

        text.insertMark(0, 5, Marks.Link, {
          tag: "link",
          url: "https://example.com",
        });

        const marks = text.resolveMarks();
        expect(marks).toHaveLength(1);
        expect(marks[0]!.sourceMark).toHaveProperty(
          "url",
          "https://example.com",
        );
      });

      test("inserting mark at text boundaries", () => {
        const text = CoRichText.createFromPlainText("hello world", {
          owner: me,
        });

        // Mark entire text
        text.insertMark(0, 10, Marks.Strong, { tag: "strong" });

        const marks = text.resolveMarks();
        expect(marks).toHaveLength(1);
        expect(marks[0]!.startAfter).toBe(-1);
        expect(marks[0]!.startBefore).toBe(0);
        expect(marks[0]!.endAfter).toBe(10);
        expect(marks[0]!.endBefore).toBe(11);
      });

      test("inserting mark outside of text bounds", () => {
        const text = CoRichText.createFromPlainText("hello world", {
          owner: me,
        });

        text.insertMark(-1, 10, Marks.Strong, { tag: "strong" });
        expect(text.resolveMarks()).toHaveLength(1);
        expect(text.resolveMarks()[0]!.startAfter).toBe(-1);
        expect(text.resolveMarks()[0]!.startBefore).toBe(0);
        expect(text.resolveMarks()[0]!.endAfter).toBe(10);
        expect(text.resolveMarks()[0]!.endBefore).toBe(11);
      });

      test("maintains correct mark ordering with nested marks", () => {
        const text = CoRichText.createFromPlainText("hello world", {
          owner: me,
        });

        text.insertMark(0, 10, Marks.Strong, { tag: "strong" }); // "hello world"
        text.insertMark(2, 8, Marks.Em, { tag: "em" }); // "llo wor"
        text.insertMark(4, 6, Marks.Link, {
          tag: "link",
          url: "https://example.com",
        }); // "o w"

        const tree = text.toTree(["strong", "em", "link"]);
        // Verify the nesting structure is correct
        // Strong should contain Em which should contain Link
        expect(tree).toEqual({
          type: "node",
          tag: "root",
          start: 0,
          end: 10,
          children: [
            {
              type: "node",
              tag: "strong",
              start: 0,
              end: 1,
              children: [
                {
                  type: "leaf",
                  start: 0,
                  end: 1,
                },
              ],
            },
            {
              type: "node",
              tag: "strong",
              start: 2,
              end: 3,
              children: [
                {
                  type: "node",
                  tag: "em",
                  start: 2,
                  end: 3,
                  children: [
                    {
                      type: "leaf",
                      start: 2,
                      end: 3,
                    },
                  ],
                },
              ],
            },
            {
              type: "node",
              tag: "strong",
              start: 4,
              end: 6,
              children: [
                {
                  type: "node",
                  tag: "em",
                  start: 4,
                  end: 6,
                  children: [
                    {
                      type: "node",
                      tag: "link",
                      start: 4,
                      end: 6,
                      children: [
                        {
                          type: "leaf",
                          start: 4,
                          end: 6,
                        },
                      ],
                    },
                  ],
                },
              ],
            },
            {
              type: "node",
              tag: "strong",
              start: 7,
              end: 8,
              children: [
                {
                  type: "node",
                  tag: "em",
                  start: 7,
                  end: 8,
                  children: [
                    {
                      type: "leaf",
                      start: 7,
                      end: 8,
                    },
                  ],
                },
              ],
            },
            {
              type: "node",
              tag: "strong",
              start: 9,
              end: 10,
              children: [
                {
                  type: "leaf",
                  start: 9,
                  end: 10,
                },
              ],
            },
          ],
        });
      });
    });

    describe("removing marks", () => {
      test("basic mark removal", () => {
        const text = CoRichText.createFromPlainText("hello world", {
          owner: me,
        });

        // Add a mark
        text.insertMark(0, 5, Marks.Strong, { tag: "strong" });

        // Verify mark was added
        expect(text.resolveMarks()).toHaveLength(1);

        // Remove the mark
        text.removeMark(0, 5, Marks.Strong, { tag: "strong" });

        // Verify mark was removed
        expect(text.resolveMarks()).toHaveLength(0);
      });

      test("skips marks that aren't in the range", () => {
        const text = CoRichText.createFromPlainText("hello world", {
          owner: me,
        });

        text.insertMark(0, 2, Marks.Strong, { tag: "strong" });
        text.removeMark(3, 6, Marks.Strong, { tag: "strong" });
        text.insertMark(7, 11, Marks.Strong, { tag: "strong" });

        expect(text.resolveMarks()).toHaveLength(2);
      });

      test("removing marks with partial overlap", () => {
        const text = CoRichText.createFromPlainText("hello world", {
          owner: me,
        });

        // Add marks at different positions
        text.insertMark(0, 6, Marks.Strong, { tag: "strong" }); // "hello "
        text.insertMark(4, 11, Marks.Strong, { tag: "strong" }); // "o world"

        // Verify initial marks
        expect(text.resolveMarks()).toHaveLength(2);

        // Remove mark in middle (4-7: "o w") where the marks overlap
        // This should trim both marks to exclude the removed region:
        // - First mark should become "hell" (was "hello ")
        // - Second mark should become "rld" (was "o world")
        text.removeMark(4, 7, Marks.Strong, { tag: "strong" });

        // Should have two marks remaining - one before and one after the removal
        const remainingMarks = text.resolveMarks();
        expect(remainingMarks).toHaveLength(2);

        // Verify the remaining marks
        // First mark should be trimmed to "hell"
        expect(remainingMarks[0]!.startAfter).toBe(-1);
        expect(remainingMarks[0]!.startBefore).toBe(0);
        expect(remainingMarks[0]!.endAfter).toBe(3);
        expect(remainingMarks[0]!.endBefore).toBe(4);

        // Second mark should be trimmed to "rld"
        expect(remainingMarks[1]!.startAfter).toBe(7);
        expect(remainingMarks[1]!.startBefore).toBe(8);
        expect(remainingMarks[1]!.endAfter).toBe(10);
        expect(remainingMarks[1]!.endBefore).toBe(11);

        // Verify the text content is still intact
        expect(text.toString()).toBe("hello world");
      });

      test("removing marks of specific type", () => {
        const text = CoRichText.createFromPlainText("hello world", {
          owner: me,
        });

        // Add different types of marks
        text.insertMark(0, 5, Marks.Strong, { tag: "strong" });
        text.insertMark(0, 5, Marks.Em, { tag: "em" });

        // Verify both marks were added
        expect(text.resolveMarks()).toHaveLength(2);

        // Remove only Strong marks
        text.removeMark(0, 5, Marks.Strong, { tag: "strong" });

        // Should have one mark remaining
        const remainingMarks = text.resolveMarks();
        expect(remainingMarks).toHaveLength(1);
        expect(remainingMarks[0]!.sourceMark!.tag).toBe("em");
      });

      test("removing mark that overlaps end of existing mark", () => {
        const text = CoRichText.createFromPlainText("hello world", {
          owner: me,
        });

        // Add mark covering "hello world"
        text.insertMark(0, 11, Marks.Strong, { tag: "strong" });

        // Remove mark covering "world"
        text.removeMark(6, 11, Marks.Strong, { tag: "strong" });

        // Should have one mark remaining on "hello "
        const remainingMarks = text.resolveMarks();
        expect(remainingMarks).toHaveLength(1);
        expect(remainingMarks[0]!.startBefore).toBe(0);
        expect(remainingMarks[0]!.endAfter).toBe(5);
      });

      test("removing mark that overlaps start of existing mark", () => {
        const text = CoRichText.createFromPlainText("hello world", {
          owner: me,
        });

        // Add mark covering "hello world"
        text.insertMark(0, 11, Marks.Strong, { tag: "strong" });

        // Remove mark covering "hello "
        text.removeMark(0, 5, Marks.Strong, { tag: "strong" });

        // Should have one mark remaining on "world"
        const remainingMarks = text.resolveMarks();
        expect(remainingMarks).toHaveLength(1);
        expect(remainingMarks[0]!.startAfter).toBe(5);
        expect(remainingMarks[0]!.startBefore).toBe(6);
        expect(remainingMarks[0]!.endAfter).toBe(10);
        expect(remainingMarks[0]!.endBefore).toBe(11);
      });

      test("removing mark from middle of existing mark", () => {
        const text = CoRichText.createFromPlainText("hello world", {
          owner: me,
        });

        // Add mark covering "hello world"
        text.insertMark(0, 10, Marks.Strong, { tag: "strong" });

        // Remove mark covering " wo"
        text.removeMark(5, 8, Marks.Strong, { tag: "strong" });

        // Should have two marks remaining on "hello" and "rld"
        const remainingMarks = text.resolveMarks();
        expect(remainingMarks).toHaveLength(2);

        // First mark should cover "hello"
        expect(remainingMarks[0]!.startAfter).toBe(-1);
        expect(remainingMarks[0]!.startBefore).toBe(0);
        expect(remainingMarks[0]!.endAfter).toBe(4);
        expect(remainingMarks[0]!.endBefore).toBe(5);

        // Second mark should cover "rld"
        expect(remainingMarks[1]!.startAfter).toBe(8);
        expect(remainingMarks[1]!.endAfter).toBe(10);
      });
    });
  });

  describe("Conversion", () => {
    test("to tree", () => {
      const text = CoRichText.createFromPlainText("hello world", {
        owner: me,
      });

      expect(text.toTree(["strong"])).toEqual({
        type: "node",
        tag: "root",
        start: 0,
        end: 10,
        children: [
          {
            type: "leaf",
            start: 0,
            end: 10,
          },
        ],
      });
    });

    test("to string", () => {
      const text = CoRichText.createFromPlainText("hello world", {
        owner: me,
      });

      expect(text.toString()).toEqual("hello world");
    });

    test("splits nested children correctly", () => {
      // Create text with nested marks
      const text = CoRichText.createFromPlainText("hello world", {
        owner: me,
      });

      // Add an outer mark spanning the whole text
      text.insertMark(0, 10, Marks.Strong, { tag: "strong" });

      // Add an inner mark spanning part of the text
      text.insertMark(6, 10, Marks.Em, { tag: "em" });

      const tree = text.toTree(["strong", "em"]);

      expect(tree).toEqual({
        type: "node",
        tag: "root",
        start: 0,
        end: 10,
        children: [
          {
            type: "node",
            tag: "strong",
            start: 0,
            end: 5,
            children: [
              {
                type: "leaf",
                start: 0,
                end: 5,
              },
            ],
          },
          {
            type: "node",
            tag: "strong",
            start: 6,
            end: 10,
            children: [
              {
                type: "node",
                tag: "em",
                start: 6,
                end: 10,
                children: [
                  {
                    type: "leaf",
                    start: 6,
                    end: 10,
                  },
                ],
              },
            ],
          },
        ],
      });

      // Now verify splitting works by checking a specific position (between 'wo' and 'rld')
      const [before, after] = splitNode(tree.children[1] as TreeNode, 8);

      // Verify the structure of the split nodes
      expect(before).toEqual({
        type: "node",
        tag: "strong",
        start: 6,
        end: 7,
        children: [
          {
            type: "node",
            tag: "em",
            start: 6,
            end: 7,
            children: [
              {
                type: "leaf",
                start: 6,
                end: 7,
              },
            ],
          },
        ],
      });

      expect(after).toEqual({
        type: "node",
        tag: "strong",
        start: 8,
        end: 10,
        children: [
          {
            type: "node",
            tag: "em",
            start: 8,
            end: 10,
            children: [
              {
                type: "leaf",
                start: 8,
                end: 10,
              },
            ],
          },
        ],
      });
    });

    test("mark boundaries are preserved in resolveAndDiffuseAndFocusMarks", () => {
      const text = CoRichText.createFromPlainText("Hello world", {
        owner: me,
      });

      // Add two non-overlapping marks
      text.insertMark(0, 4, Marks.Strong, { tag: "strong" }); // "Hello"
      text.insertMark(6, 10, Marks.Strong, { tag: "strong" }); // "world"

      const marks = text.resolveAndDiffuseAndFocusMarks();
      expect(marks).toHaveLength(2);

      // Verify we have marks before proceeding
      expect(marks[0]).toBeDefined();
      expect(marks[1]).toBeDefined();

      if (!marks[0] || !marks[1]) return;

      // Check first mark boundaries
      expect(marks[0].start).toBe(0);
      expect(marks[0].end).toBe(4);

      // Check second mark boundaries
      expect(marks[1].start).toBe(6);
      expect(marks[1].end).toBe(10);
    });

    test("certainMiddle regions should not expand into uncertain territory", () => {
      const text = CoRichText.createFromPlainText("Hello world", {
        owner: me,
      });

      // Mark "Hello" with strong
      text.insertMark(0, 4, Marks.Strong, { tag: "strong" }); // "Hell"

      const diffusedMarks = text.resolveAndDiffuseMarks();

      // Find the certainMiddle region
      const certainMiddle = diffusedMarks.find(
        (m) => m.side === "certainMiddle",
      );
      expect(certainMiddle).toBeDefined();

      if (!certainMiddle) return;

      // The certainMiddle region should exactly match the marked region
      expect(certainMiddle.start).toBe(0);
      expect(certainMiddle.end).toBe(4);

      // The tree should reflect the exact marked region
      const tree = text.toTree(["strong"]);
      const strongNode = tree.children[0] as TreeNode;

      // The tree node should include exactly "Hell"
      const markedText = text
        .toString()
        .substring(strongNode.start, strongNode.end);
      expect(markedText).toBe("Hell");
    });
  });

  describe("Resolution", () => {
    const initNodeAndText = async () => {
      const me = await Account.create({
        creationProps: { name: "Hermes Puggington" },
        crypto: Crypto,
      });

      const text = CoRichText.createFromPlainText("hello world", {
        owner: me,
      });

      return { me, text };
    };

    test("Loading and availability", async () => {
      const { me, text } = await initNodeAndText();

      const [initialAsPeer, secondPeer] = connectedPeers("initial", "second", {
        peer1role: "server",
        peer2role: "client",
      });
      if (!isControlledAccount(me)) {
        throw "me is not a controlled account";
      }
      me._raw.core.node.syncManager.addPeer(secondPeer);
      const { account: meOnSecondPeer } = await createJazzContext({
        auth: fixedCredentialsAuth({
          accountID: me.id,
          secret: me._raw.agentSecret,
        }),
        sessionProvider: randomSessionProvider,
        peersToLoadFrom: [initialAsPeer],
        crypto: Crypto,
      });

      const loadedText = await CoRichText.load(text.id, meOnSecondPeer, {
        marks: [{}],
        text: [],
      });

      expect(loadedText).toBeDefined();
      expect(loadedText?.toString()).toEqual("hello world");

      const loadedText2 = await CoRichText.load(text.id, meOnSecondPeer, {
        marks: [{}],
        text: [],
      });

      expect(loadedText2).toBeDefined();
      expect(loadedText2?.toString()).toEqual("hello world");
    });

    test("Subscription & auto-resolution", async () => {
      const { me, text } = await initNodeAndText();

      const [initialAsPeer, secondPeer] = connectedPeers("initial", "second", {
        peer1role: "server",
        peer2role: "client",
      });

      if (!isControlledAccount(me)) {
        throw "me is not a controlled account";
      }
      me._raw.core.node.syncManager.addPeer(secondPeer);
      const { account: meOnSecondPeer } = await createJazzContext({
        auth: fixedCredentialsAuth({
          accountID: me.id,
          secret: me._raw.agentSecret,
        }),
        sessionProvider: randomSessionProvider,
        peersToLoadFrom: [initialAsPeer],
        crypto: Crypto,
      });

      const queue = new cojsonInternals.Channel<CoRichText>();

      CoRichText.subscribe(
        text.id,
        meOnSecondPeer,
        { marks: [{}], text: [] },
        (subscribedText) => {
          void queue.push(subscribedText);
        },
      );

      const update1 = (await queue.next()).value;
      expect(update1.toString()).toBe("hello world");

      text.insertAfter(4, " beautiful");
      const update2 = (await queue.next()).value;
      expect(update2.toString()).toBe("hello beautiful world");

      text.deleteRange({ from: 5, to: 15 });
      const update3 = (await queue.next()).value;
      expect(update3.toString()).toBe("hello world");

      text.insertMark(0, 11, Marks.Strong, { tag: "strong" });
      const update4 = (await queue.next()).value;
      expect(update4.toString()).toBe("hello world");
      expect(update4.resolveMarks()).toHaveLength(1);
      expect(update4.resolveMarks()[0]!.tag).toBe("strong");

      text.removeMark(5, 11, Marks.Strong, { tag: "strong" });
      const update5 = (await queue.next()).value;
      expect(update5.toString()).toBe("hello world");
      expect(update5.resolveMarks()).toHaveLength(1);
      expect(update5.resolveMarks()[0]!.tag).toBe("strong");
      expect(update5.resolveMarks()[0]!.startAfter).toBe(-1);
      expect(update5.resolveMarks()[0]!.startBefore).toBe(0);
      expect(update5.resolveMarks()[0]!.endAfter).toBe(4);
      expect(update5.resolveMarks()[0]!.endBefore).toBe(5);
    });
  });

  // In the sense of the mark resolving in the text, not sync resolution
  describe("Mark resolution", () => {
    test("basic position resolution", () => {
      const text = CoRichText.createFromPlainText("hello world", {
        owner: me,
      });

      // Create mark directly
      const mark = Marks.Strong.create(
        {
          tag: "strong",
          startAfter: text.posAfter(-1) as TextPos,
          startBefore: text.posBefore(0) as TextPos,
          endAfter: text.posAfter(4) as TextPos,
          endBefore: text.posBefore(5) as TextPos,
        },
        { owner: me },
      );

      // Add mark directly to marks list
      text.marks!.push(mark);

      const marks = text.resolveMarks();
      expect(marks).toHaveLength(1);
      expect(marks[0]).toMatchObject({
        startAfter: -1,
        startBefore: 0,
        endAfter: 4,
        endBefore: 5,
        tag: "strong",
      });
    });

    test("handles multiple marks", () => {
      const text = CoRichText.createFromPlainText("hello world", {
        owner: me,
      });

      // Create marks directly
      const mark1 = Marks.Strong.create(
        {
          tag: "strong",
          startAfter: text.posAfter(-1) as TextPos,
          startBefore: text.posBefore(0) as TextPos,
          endAfter: text.posAfter(4) as TextPos,
          endBefore: text.posBefore(5) as TextPos,
        },
        { owner: me },
      );

      const mark2 = Marks.Em.create(
        {
          tag: "em",
          startAfter: text.posAfter(6) as TextPos,
          startBefore: text.posBefore(7) as TextPos,
          endAfter: text.posAfter(10) as TextPos,
          endBefore: text.posBefore(11) as TextPos,
        },
        { owner: me },
      );

      // Add marks directly
      text.marks!.push(mark1);
      text.marks!.push(mark2);

      const marks = text.resolveMarks();
      expect(marks).toHaveLength(2);

      // First mark
      expect(marks[0]).toMatchObject({
        startAfter: -1,
        startBefore: 0,
        endAfter: 4,
        endBefore: 5,
        tag: "strong",
      });

      // Second mark
      expect(marks[1]).toMatchObject({
        startAfter: 6,
        startBefore: 7,
        endAfter: 10,
        endBefore: 11,
        tag: "em",
      });
    });

    test("handles overlapping marks", () => {
      const text = CoRichText.createFromPlainText("hello world", {
        owner: me,
      });

      // Create overlapping marks directly
      const mark1 = Marks.Strong.create(
        {
          tag: "strong",
          startAfter: text.posAfter(-1) as TextPos,
          startBefore: text.posBefore(0) as TextPos,
          endAfter: text.posAfter(4) as TextPos,
          endBefore: text.posBefore(5) as TextPos,
        },
        { owner: me },
      );

      const mark2 = Marks.Em.create(
        {
          tag: "em",
          startAfter: text.posAfter(3) as TextPos,
          startBefore: text.posBefore(4) as TextPos,
          endAfter: text.posAfter(7) as TextPos,
          endBefore: text.posBefore(8) as TextPos,
        },
        { owner: me },
      );

      // Add marks directly
      text.marks!.push(mark1);
      text.marks!.push(mark2);

      const marks = text.resolveMarks();

      expect(marks).toHaveLength(2);

      // First mark
      expect(marks[0]).toMatchObject({
        startAfter: -1,
        startBefore: 0,
        endAfter: 4,
        endBefore: 5,
        tag: "strong",
      });

      // Second mark
      expect(marks[1]).toMatchObject({
        startAfter: 3,
        startBefore: 4,
        endAfter: 7,
        endBefore: 8,
        tag: "em",
      });
    });
  });

  describe("Mark", () => {
    const text = CoRichText.createFromPlainText(
      "Hello, hello you beautiful world", // 32 characters
      {
        owner: me,
      },
    );

    test("basic mark", () => {
      const mark = Marks.Strong.create(
        {
          tag: "strong",
          startAfter: text.posAfter(0) as TextPos,
          startBefore: text.posBefore(1) as TextPos,
          endAfter: text.posAfter(4) as TextPos,
          endBefore: text.posBefore(5) as TextPos,
        },
        { owner: me },
      );
      expect(mark.tag).toEqual("strong");
    });

    test("validates positions correctly", () => {
      const mark = Marks.Strong.create(
        {
          tag: "strong",
          startAfter: text.posAfter(-1) as TextPos,
          startBefore: text.posBefore(0) as TextPos,
          endAfter: text.posAfter(4) as TextPos,
          endBefore: text.posBefore(5) as TextPos,
        },
        { owner: me },
      );

      const result = mark.validatePositions(
        text.length,
        (pos: TextPos) => text.idxAfter(pos),
        (pos: TextPos) => text.idxBefore(pos),
      );

      expect(result).toEqual({
        startAfter: -1,
        startBefore: 0,
        endAfter: 4,
        endBefore: 5,
      });
    });

    test("clamps positions to text bounds", () => {
      const mark = Marks.Strong.create(
        {
          tag: "strong",
          startAfter: text.posAfter(-5) as TextPos, // Invalid position
          startBefore: text.posBefore(1) as TextPos,
          endAfter: text.posAfter(35) as TextPos,
          endBefore: text.posBefore(36) as TextPos, // Beyond text length
        },
        { owner: me },
      );

      const result = mark.validatePositions(
        text.length,
        (pos) => text.idxAfter(pos),
        (pos) => text.idxBefore(pos),
      );

      expect(result).toMatchObject({
        startAfter: 0,
        startBefore: 1,
        endAfter: 31,
        endBefore: 32,
      });
    });

    test("different mark types have correct tags", () => {
      const strongMark = Marks.Strong.create(
        {
          tag: "strong",
          startAfter: text.posAfter(0) as TextPos,
          startBefore: text.posBefore(1) as TextPos,
          endAfter: text.posAfter(4) as TextPos,
          endBefore: text.posBefore(5) as TextPos,
        },
        { owner: me },
      );

      const emMark = Marks.Em.create(
        {
          tag: "em",
          startAfter: text.posAfter(0) as TextPos,
          startBefore: text.posBefore(1) as TextPos,
          endAfter: text.posAfter(4) as TextPos,
          endBefore: text.posBefore(5) as TextPos,
        },
        { owner: me },
      );

      const linkMark = Marks.Link.create(
        {
          tag: "link",
          url: "https://example.com",
          startAfter: text.posAfter(0) as TextPos,
          startBefore: text.posBefore(1) as TextPos,
          endAfter: text.posAfter(4) as TextPos,
          endBefore: text.posBefore(5) as TextPos,
        },
        { owner: me },
      );

      expect(strongMark.tag).toBe("strong");
      expect(emMark.tag).toBe("em");
      expect(linkMark.tag).toBe("link");
      expect(linkMark).toHaveProperty("url", "https://example.com");
    });

    describe("validatePositions", () => {
      test("returns null for empty text", () => {
        const mark = Marks.Strong.create(
          {
            tag: "strong",
            startAfter: text.posAfter(0) as TextPos,
            startBefore: text.posBefore(1) as TextPos,
            endAfter: text.posAfter(4) as TextPos,
            endBefore: text.posBefore(5) as TextPos,
          },
          { owner: me },
        );
        expect(
          mark.validatePositions(
            0,
            (pos) => text.idxAfter(pos),
            (pos) => text.idxBefore(pos),
          ),
        ).toBe(null);
      });

      test("enforces consistent uncertainty regions", () => {
        const mark = Marks.Strong.create(
          {
            tag: "strong",
            startAfter: text.posAfter(5) as TextPos,
            startBefore: text.posBefore(6) as TextPos,
            endAfter: text.posAfter(10) as TextPos,
            endBefore: text.posBefore(11) as TextPos,
          },
          { owner: me },
        );

        const result = mark.validatePositions(
          text.length,
          (pos) => text.idxAfter(pos),
          (pos) => text.idxBefore(pos),
        );
        expect(result).toEqual({
          startAfter: 5,
          startBefore: 6,
          endAfter: 10,
          endBefore: 11,
        });
      });

      test("enforces minimum uncertainty region size", () => {
        // Try to set positions with 0-unit uncertainty regions
        const mark = Marks.Strong.create(
          {
            tag: "strong",
            startAfter: text.posAfter(5) as TextPos,
            startBefore: text.posBefore(5) as TextPos, // Same as startAfter
            endAfter: text.posAfter(10) as TextPos,
            endBefore: text.posBefore(10) as TextPos, // Same as endAfter
          },
          { owner: me },
        );

        const result = mark.validatePositions(
          text.length,
          (pos) => text.idxAfter(pos),
          (pos) => text.idxBefore(pos),
        );
        expect(result).toEqual({
          startAfter: 4,
          startBefore: 5,
          endAfter: 10,
          endBefore: 11,
        });
      });

      test("clamps positions to text bounds", () => {
        // Set positions outside text bounds
        const mark = Marks.Strong.create(
          {
            tag: "strong",
            startAfter: text.posAfter(-1) as TextPos,
            startBefore: text.posBefore(0) as TextPos,
            endAfter: text.posAfter(35) as TextPos,
            endBefore: text.posBefore(36) as TextPos,
          },
          { owner: me },
        );

        const result = mark.validatePositions(
          text.length,
          (pos) => text.idxAfter(pos),
          (pos) => text.idxBefore(pos),
        );
        expect(result).toEqual({
          startAfter: -1,
          startBefore: 0,
          endAfter: 31,
          endBefore: 32,
        });
      });

      test("maintains proper position ordering", () => {
        // Set positions in wrong order
        const mark = Marks.Strong.create(
          {
            tag: "strong",
            startAfter: text.posAfter(8) as TextPos,
            startBefore: text.posBefore(5) as TextPos,
            endAfter: text.posAfter(15) as TextPos, //
            endBefore: text.posBefore(14) as TextPos,
          },
          { owner: me },
        );

        const result = mark.validatePositions(
          text.length,
          (pos) => text.idxAfter(pos),
          (pos) => text.idxBefore(pos),
        );
        expect(result).toEqual({
          startAfter: 4, // Forced to be before startBefore
          startBefore: 5,
          endAfter: 15,
          endBefore: 16, // Forced to be after endAfter
        });
      });
    });
  });
});
