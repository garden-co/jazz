import { assert, beforeEach, describe, expect, test } from "vitest";
import {
  fillCoMapWithLargeData,
  loadCoValueOrFail,
  setupTestNode,
} from "./testUtils.js";
import { CO_VALUE_PRIORITY } from "../priority.js";
import { emptyKnownState, SessionID } from "../exports.js";
import { NewContentMessage } from "../sync.js";
import { addTransactionToContentMessage } from "../coValueContentMessage.js";

beforeEach(() => {
  setupTestNode({ isSyncServer: true });
});

// Simplify the content message to a value that can be used for snapshots
function simplifyContentMessage(
  message: NewContentMessage,
  sessionMapping: Record<string, string>,
) {
  const newContent = Object.entries(message.new).map(([sessionID, content]) => [
    sessionMapping[sessionID],
    {
      lastSignature: content.lastSignature ? "signature" : undefined,
      newTransactions: `${content.newTransactions.length} transactions after ${content.after}`,
    },
  ]);

  return {
    action: message.action,
    id: message.id ? "id" : undefined,
    header: message.header ? "header" : undefined,
    priority: message.priority,
    new: Object.fromEntries(newContent),
    expectContentUntil: message.expectContentUntil
      ? Object.fromEntries(
          Object.entries(message.expectContentUntil).map(
            ([sessionID, after]) => [sessionMapping[sessionID], after],
          ),
        )
      : undefined,
  };
}

function simplifyContentMessages(
  messages: NewContentMessage[] | undefined,
  sessionMapping: Record<string, string>,
) {
  if (!messages) {
    return undefined;
  }
  return messages.map((message) =>
    simplifyContentMessage(message, sessionMapping),
  );
}

describe("newContentSince", () => {
  test("should return an empty content with the header when both the coValue and knownState are empty", () => {
    const client = setupTestNode();

    const group = client.node.createGroup();
    const map = group.createMap();

    expect(map.core.newContentSince(emptyKnownState(map.core.id))).toEqual([
      {
        action: "content",
        id: map.core.id,
        header: map.core.verified.header,
        priority: CO_VALUE_PRIORITY.MEDIUM,
        new: {},
      },
    ]);
  });

  test("should return an empty content with the header when the coValue is empty and knownState is undefined", () => {
    const client = setupTestNode();

    const group = client.node.createGroup();
    const map = group.createMap();

    expect(map.core.newContentSince(emptyKnownState(map.core.id))).toEqual([
      {
        action: "content",
        id: map.core.id,
        header: map.core.verified.header,
        priority: CO_VALUE_PRIORITY.MEDIUM,
        new: {},
      },
    ]);
  });

  test("should return the content when the coValue has new content and knownState is undefined", () => {
    const client = setupTestNode();

    const group = client.node.createGroup();
    const map = group.createMap();

    map.set("hello", "world", "trusting");

    const content = map.core.newContentSince(undefined);

    const sessionMapping: Record<string, string> = {
      [client.node.currentSessionID]: "alice",
    };

    expect(
      simplifyContentMessages(content, sessionMapping),
    ).toMatchInlineSnapshot(`
      [
        {
          "action": "content",
          "expectContentUntil": undefined,
          "header": "header",
          "id": "id",
          "new": {
            "alice": {
              "lastSignature": "signature",
              "newTransactions": "1 transactions after 0",
            },
          },
          "priority": 3,
        },
      ]
    `);
  });

  test("should return the content when the coValue has new content from multiple sessions and knownState is undefined", async () => {
    const client = setupTestNode({
      connected: true,
    });

    const group = client.node.createGroup();
    const map = group.createMap();

    map.set("hello", "world", "trusting");

    const newSession = client.spawnNewSession();

    const mapInNewSession = await loadCoValueOrFail(newSession.node, map.id);

    mapInNewSession.set("hello", "world2", "trusting");

    const content = mapInNewSession.core.newContentSince(undefined);

    const sessionMapping: Record<string, string> = {
      [client.node.currentSessionID]: "alice",
      [newSession.node.currentSessionID]: "bob",
    };

    expect(
      simplifyContentMessages(content, sessionMapping),
    ).toMatchInlineSnapshot(`
      [
        {
          "action": "content",
          "expectContentUntil": undefined,
          "header": "header",
          "id": "id",
          "new": {
            "alice": {
              "lastSignature": "signature",
              "newTransactions": "1 transactions after 0",
            },
            "bob": {
              "lastSignature": "signature",
              "newTransactions": "1 transactions after 0",
            },
          },
          "priority": 3,
        },
      ]
    `);
  });

  describe("large session logs", () => {
    test("one large session log that requires multiple content pieces", () => {
      const client = setupTestNode();

      const group = client.node.createGroup();
      const map = group.createMap();

      fillCoMapWithLargeData(map);

      const content = map.core.newContentSince(undefined);

      const sessionMapping: Record<string, string> = {
        [client.node.currentSessionID]: "alice",
      };

      expect(
        simplifyContentMessages(content, sessionMapping),
      ).toMatchInlineSnapshot(`
        [
          {
            "action": "content",
            "expectContentUntil": {
              "alice": 200,
            },
            "header": "header",
            "id": "id",
            "new": {
              "alice": {
                "lastSignature": "signature",
                "newTransactions": "73 transactions after 0",
              },
            },
            "priority": 3,
          },
          {
            "action": "content",
            "expectContentUntil": undefined,
            "header": undefined,
            "id": "id",
            "new": {
              "alice": {
                "lastSignature": "signature",
                "newTransactions": "73 transactions after 73",
              },
            },
            "priority": 3,
          },
          {
            "action": "content",
            "expectContentUntil": undefined,
            "header": undefined,
            "id": "id",
            "new": {
              "alice": {
                "lastSignature": "signature",
                "newTransactions": "54 transactions after 146",
              },
            },
            "priority": 3,
          },
        ]
      `);
    });

    test("one large session log that requires multiple content pieces and one small", async () => {
      const client = setupTestNode({
        connected: true,
      });

      const group = client.node.createGroup();
      const map = group.createMap();

      // Generate a large amount of data (about 1MB)
      fillCoMapWithLargeData(map);

      // Add a small session
      const newSession = client.spawnNewSession();
      const mapInNewSession = await loadCoValueOrFail(newSession.node, map.id);
      mapInNewSession.set("small", "value", "trusting");

      const content = mapInNewSession.core.newContentSince(undefined);

      const sessionMapping: Record<string, string> = {
        [client.node.currentSessionID]: "alice",
        [newSession.node.currentSessionID]: "bob",
      };

      expect(
        simplifyContentMessages(content, sessionMapping),
      ).toMatchInlineSnapshot(`
        [
          {
            "action": "content",
            "expectContentUntil": {
              "alice": 200,
              "bob": 1,
            },
            "header": "header",
            "id": "id",
            "new": {
              "alice": {
                "lastSignature": "signature",
                "newTransactions": "73 transactions after 0",
              },
            },
            "priority": 3,
          },
          {
            "action": "content",
            "expectContentUntil": undefined,
            "header": undefined,
            "id": "id",
            "new": {
              "alice": {
                "lastSignature": "signature",
                "newTransactions": "73 transactions after 73",
              },
            },
            "priority": 3,
          },
          {
            "action": "content",
            "expectContentUntil": undefined,
            "header": undefined,
            "id": "id",
            "new": {
              "alice": {
                "lastSignature": "signature",
                "newTransactions": "54 transactions after 146",
              },
              "bob": {
                "lastSignature": "signature",
                "newTransactions": "1 transactions after 0",
              },
            },
            "priority": 3,
          },
        ]
      `);
    });

    test("two large session logs", async () => {
      const client = setupTestNode({
        connected: true,
      });

      const group = client.node.createGroup();
      const map = group.createMap();

      // Generate a large amount of data in first session (about 1MB)
      fillCoMapWithLargeData(map);

      // Add second large session
      const newSession = client.spawnNewSession();
      const mapInNewSession = await loadCoValueOrFail(newSession.node, map.id);

      fillCoMapWithLargeData(mapInNewSession);

      const content = mapInNewSession.core.newContentSince(undefined);

      const sessionMapping: Record<string, string> = {
        [client.node.currentSessionID]: "alice",
        [newSession.node.currentSessionID]: "bob",
      };

      expect(
        simplifyContentMessages(content, sessionMapping),
      ).toMatchInlineSnapshot(`
        [
          {
            "action": "content",
            "expectContentUntil": {
              "alice": 200,
              "bob": 200,
            },
            "header": "header",
            "id": "id",
            "new": {
              "alice": {
                "lastSignature": "signature",
                "newTransactions": "73 transactions after 0",
              },
            },
            "priority": 3,
          },
          {
            "action": "content",
            "expectContentUntil": undefined,
            "header": undefined,
            "id": "id",
            "new": {
              "alice": {
                "lastSignature": "signature",
                "newTransactions": "73 transactions after 73",
              },
            },
            "priority": 3,
          },
          {
            "action": "content",
            "expectContentUntil": undefined,
            "header": undefined,
            "id": "id",
            "new": {
              "bob": {
                "lastSignature": "signature",
                "newTransactions": "73 transactions after 0",
              },
            },
            "priority": 3,
          },
          {
            "action": "content",
            "expectContentUntil": undefined,
            "header": undefined,
            "id": "id",
            "new": {
              "bob": {
                "lastSignature": "signature",
                "newTransactions": "73 transactions after 73",
              },
            },
            "priority": 3,
          },
          {
            "action": "content",
            "expectContentUntil": undefined,
            "header": undefined,
            "id": "id",
            "new": {
              "alice": {
                "lastSignature": "signature",
                "newTransactions": "54 transactions after 146",
              },
              "bob": {
                "lastSignature": "signature",
                "newTransactions": "54 transactions after 146",
              },
            },
            "priority": 3,
          },
        ]
      `);
    });

    test("one large, one small, and one large session log", async () => {
      const client = setupTestNode({
        connected: true,
      });

      const group = client.node.createGroup();
      const map = group.createMap();

      fillCoMapWithLargeData(map);

      // Add small session
      const session2 = client.spawnNewSession();
      const mapInSession2 = await loadCoValueOrFail(session2.node, map.id);
      mapInSession2.set("small", "value", "trusting");

      // Add third large session
      const session3 = client.spawnNewSession();
      const mapInSession3 = await loadCoValueOrFail(session3.node, map.id);

      fillCoMapWithLargeData(mapInSession3);

      const content = mapInSession3.core.newContentSince(undefined);

      const sessionMapping: Record<string, string> = {
        [client.node.currentSessionID]: "alice",
        [session2.node.currentSessionID]: "bob",
        [session3.node.currentSessionID]: "charlie",
      };

      expect(
        simplifyContentMessages(content, sessionMapping),
      ).toMatchInlineSnapshot(`
        [
          {
            "action": "content",
            "expectContentUntil": {
              "alice": 200,
              "bob": 1,
              "charlie": 200,
            },
            "header": "header",
            "id": "id",
            "new": {
              "alice": {
                "lastSignature": "signature",
                "newTransactions": "73 transactions after 0",
              },
            },
            "priority": 3,
          },
          {
            "action": "content",
            "expectContentUntil": undefined,
            "header": undefined,
            "id": "id",
            "new": {
              "alice": {
                "lastSignature": "signature",
                "newTransactions": "73 transactions after 73",
              },
            },
            "priority": 3,
          },
          {
            "action": "content",
            "expectContentUntil": undefined,
            "header": undefined,
            "id": "id",
            "new": {
              "charlie": {
                "lastSignature": "signature",
                "newTransactions": "73 transactions after 0",
              },
            },
            "priority": 3,
          },
          {
            "action": "content",
            "expectContentUntil": undefined,
            "header": undefined,
            "id": "id",
            "new": {
              "charlie": {
                "lastSignature": "signature",
                "newTransactions": "73 transactions after 73",
              },
            },
            "priority": 3,
          },
          {
            "action": "content",
            "expectContentUntil": undefined,
            "header": undefined,
            "id": "id",
            "new": {
              "alice": {
                "lastSignature": "signature",
                "newTransactions": "54 transactions after 146",
              },
              "bob": {
                "lastSignature": "signature",
                "newTransactions": "1 transactions after 0",
              },
              "charlie": {
                "lastSignature": "signature",
                "newTransactions": "54 transactions after 146",
              },
            },
            "priority": 3,
          },
        ]
      `);
    });

    test("one small session log and one large without inbetween signatures", async () => {
      const client = setupTestNode({
        connected: true,
      });
      const group = client.node.createGroup();

      const map = group.createMap();
      await map.core.waitForSync();
      client.disconnect();

      // Generate first large session (about 1MB)
      fillCoMapWithLargeData(map);

      const largeContent = map.core.newContentSince(undefined);
      assert(largeContent);

      const singleLargeContentPiece = largeContent[0]!;

      for (const chunk of largeContent.slice(1)) {
        for (const [sessionID, content] of Object.entries(chunk.new)) {
          for (const tx of content.newTransactions) {
            addTransactionToContentMessage(
              singleLargeContentPiece,
              tx,
              sessionID as any,
              content.lastSignature,
              1,
            );
          }
        }
      }

      // Add small session
      const session2 = client.spawnNewSession();
      const mapInSession2 = await loadCoValueOrFail(session2.node, map.id);
      mapInSession2.set("small", "value", "trusting");
      session2.node.syncManager.handleNewContent(
        singleLargeContentPiece,
        "storage",
      );

      await mapInSession2.core.waitForSync();

      // Add third large session
      const session3 = client.spawnNewSession();
      const mapInSession3 = await loadCoValueOrFail(session3.node, map.id);

      fillCoMapWithLargeData(mapInSession3);

      const content = mapInSession3.core.newContentSince(undefined);

      const sessionMapping: Record<string, string> = {
        [client.node.currentSessionID]: "alice",
        [session2.node.currentSessionID]: "bob",
        [session3.node.currentSessionID]: "charlie",
      };

      expect(
        simplifyContentMessages(content, sessionMapping),
      ).toMatchInlineSnapshot(`
        [
          {
            "action": "content",
            "expectContentUntil": {
              "alice": 200,
              "bob": 1,
              "charlie": 200,
            },
            "header": "header",
            "id": "id",
            "new": {
              "alice": {
                "lastSignature": "signature",
                "newTransactions": "200 transactions after 0",
              },
            },
            "priority": 3,
          },
          {
            "action": "content",
            "expectContentUntil": undefined,
            "header": undefined,
            "id": "id",
            "new": {
              "charlie": {
                "lastSignature": "signature",
                "newTransactions": "73 transactions after 0",
              },
            },
            "priority": 3,
          },
          {
            "action": "content",
            "expectContentUntil": undefined,
            "header": undefined,
            "id": "id",
            "new": {
              "charlie": {
                "lastSignature": "signature",
                "newTransactions": "73 transactions after 73",
              },
            },
            "priority": 3,
          },
          {
            "action": "content",
            "expectContentUntil": undefined,
            "header": undefined,
            "id": "id",
            "new": {
              "bob": {
                "lastSignature": "signature",
                "newTransactions": "1 transactions after 0",
              },
              "charlie": {
                "lastSignature": "signature",
                "newTransactions": "54 transactions after 146",
              },
            },
            "priority": 3,
          },
        ]
      `);
    });
  });

  describe("large session logs with knownState", () => {
    test("one large session log that requires multiple content pieces starting from knownState", () => {
      const client = setupTestNode();

      const group = client.node.createGroup();
      const map = group.createMap();

      // Generate a large amount of data (about 1MB)
      fillCoMapWithLargeData(map);

      // Capture knownState partway through
      const knownState = map.core.knownState();

      // Add more data
      fillCoMapWithLargeData(map);

      const content = map.core.newContentSince(knownState);

      const sessionMapping: Record<string, string> = {
        [client.node.currentSessionID]: "alice",
      };

      expect(
        simplifyContentMessages(content, sessionMapping),
      ).toMatchInlineSnapshot(`
        [
          {
            "action": "content",
            "expectContentUntil": {
              "alice": 400,
            },
            "header": undefined,
            "id": "id",
            "new": {
              "alice": {
                "lastSignature": "signature",
                "newTransactions": "19 transactions after 200",
              },
            },
            "priority": 3,
          },
          {
            "action": "content",
            "expectContentUntil": undefined,
            "header": undefined,
            "id": "id",
            "new": {
              "alice": {
                "lastSignature": "signature",
                "newTransactions": "73 transactions after 219",
              },
            },
            "priority": 3,
          },
          {
            "action": "content",
            "expectContentUntil": undefined,
            "header": undefined,
            "id": "id",
            "new": {
              "alice": {
                "lastSignature": "signature",
                "newTransactions": "73 transactions after 292",
              },
            },
            "priority": 3,
          },
          {
            "action": "content",
            "expectContentUntil": undefined,
            "header": undefined,
            "id": "id",
            "new": {
              "alice": {
                "lastSignature": "signature",
                "newTransactions": "35 transactions after 365",
              },
            },
            "priority": 3,
          },
        ]
      `);
    });

    test("one large session log that requires multiple content pieces and one small starting from knownState", async () => {
      const client = setupTestNode({
        connected: true,
      });

      const group = client.node.createGroup();
      const map = group.createMap();

      // Generate a large amount of data (about 1MB)
      fillCoMapWithLargeData(map);

      // Capture knownState
      const knownState = map.core.knownState();

      // Add more data to first session
      fillCoMapWithLargeData(map);

      // Add a small session
      const newSession = client.spawnNewSession();
      const mapInNewSession = await loadCoValueOrFail(newSession.node, map.id);
      mapInNewSession.set("small", "value", "trusting");

      const content = mapInNewSession.core.newContentSince(knownState);

      const sessionMapping: Record<string, string> = {
        [client.node.currentSessionID]: "alice",
        [newSession.node.currentSessionID]: "bob",
      };

      expect(
        simplifyContentMessages(content, sessionMapping),
      ).toMatchInlineSnapshot(`
        [
          {
            "action": "content",
            "expectContentUntil": {
              "alice": 400,
              "bob": 1,
            },
            "header": undefined,
            "id": "id",
            "new": {
              "alice": {
                "lastSignature": "signature",
                "newTransactions": "19 transactions after 200",
              },
            },
            "priority": 3,
          },
          {
            "action": "content",
            "expectContentUntil": undefined,
            "header": undefined,
            "id": "id",
            "new": {
              "alice": {
                "lastSignature": "signature",
                "newTransactions": "73 transactions after 219",
              },
            },
            "priority": 3,
          },
          {
            "action": "content",
            "expectContentUntil": undefined,
            "header": undefined,
            "id": "id",
            "new": {
              "alice": {
                "lastSignature": "signature",
                "newTransactions": "73 transactions after 292",
              },
            },
            "priority": 3,
          },
          {
            "action": "content",
            "expectContentUntil": undefined,
            "header": undefined,
            "id": "id",
            "new": {
              "alice": {
                "lastSignature": "signature",
                "newTransactions": "35 transactions after 365",
              },
              "bob": {
                "lastSignature": "signature",
                "newTransactions": "1 transactions after 0",
              },
            },
            "priority": 3,
          },
        ]
      `);
    });

    test("two large session logs starting from knownState", async () => {
      const client = setupTestNode({
        connected: true,
      });

      const group = client.node.createGroup();
      const map = group.createMap();

      // Generate initial data
      map.set("initial", "value", "trusting");

      // Capture knownState
      const knownState = map.core.knownState();

      // Generate a large amount of data in first session (about 1MB)
      fillCoMapWithLargeData(map);

      // Add second large session
      const newSession = client.spawnNewSession();
      const mapInNewSession = await loadCoValueOrFail(newSession.node, map.id);

      fillCoMapWithLargeData(mapInNewSession);

      const content = mapInNewSession.core.newContentSince(knownState);

      const sessionMapping: Record<string, string> = {
        [client.node.currentSessionID]: "alice",
        [newSession.node.currentSessionID]: "bob",
      };

      expect(
        simplifyContentMessages(content, sessionMapping),
      ).toMatchInlineSnapshot(`
        [
          {
            "action": "content",
            "expectContentUntil": {
              "alice": 201,
              "bob": 200,
            },
            "header": undefined,
            "id": "id",
            "new": {
              "alice": {
                "lastSignature": "signature",
                "newTransactions": "73 transactions after 1",
              },
            },
            "priority": 3,
          },
          {
            "action": "content",
            "expectContentUntil": undefined,
            "header": undefined,
            "id": "id",
            "new": {
              "alice": {
                "lastSignature": "signature",
                "newTransactions": "73 transactions after 74",
              },
            },
            "priority": 3,
          },
          {
            "action": "content",
            "expectContentUntil": undefined,
            "header": undefined,
            "id": "id",
            "new": {
              "bob": {
                "lastSignature": "signature",
                "newTransactions": "73 transactions after 0",
              },
            },
            "priority": 3,
          },
          {
            "action": "content",
            "expectContentUntil": undefined,
            "header": undefined,
            "id": "id",
            "new": {
              "bob": {
                "lastSignature": "signature",
                "newTransactions": "73 transactions after 73",
              },
            },
            "priority": 3,
          },
          {
            "action": "content",
            "expectContentUntil": undefined,
            "header": undefined,
            "id": "id",
            "new": {
              "alice": {
                "lastSignature": "signature",
                "newTransactions": "54 transactions after 147",
              },
              "bob": {
                "lastSignature": "signature",
                "newTransactions": "54 transactions after 146",
              },
            },
            "priority": 3,
          },
        ]
      `);
    });

    test("one large, one small, and one large session log starting from knownState", async () => {
      const client = setupTestNode({
        connected: true,
      });

      const group = client.node.createGroup();
      const map = group.createMap();

      // Generate initial data
      map.set("initial", "value", "trusting");

      // Capture knownState
      const knownState = map.core.knownState();

      // Generate first large session (about 1MB)
      fillCoMapWithLargeData(map);

      // Add small session
      const session2 = client.spawnNewSession();
      const mapInSession2 = await loadCoValueOrFail(session2.node, map.id);
      mapInSession2.set("small", "value", "trusting");

      // Add third large session
      const session3 = client.spawnNewSession();
      const mapInSession3 = await loadCoValueOrFail(session3.node, map.id);

      fillCoMapWithLargeData(mapInSession3);

      const content = mapInSession3.core.newContentSince(knownState);

      const sessionMapping: Record<string, string> = {
        [client.node.currentSessionID]: "alice",
        [session2.node.currentSessionID]: "bob",
        [session3.node.currentSessionID]: "charlie",
      };

      expect(
        simplifyContentMessages(content, sessionMapping),
      ).toMatchInlineSnapshot(`
        [
          {
            "action": "content",
            "expectContentUntil": {
              "alice": 201,
              "bob": 1,
              "charlie": 200,
            },
            "header": undefined,
            "id": "id",
            "new": {
              "alice": {
                "lastSignature": "signature",
                "newTransactions": "73 transactions after 1",
              },
            },
            "priority": 3,
          },
          {
            "action": "content",
            "expectContentUntil": undefined,
            "header": undefined,
            "id": "id",
            "new": {
              "alice": {
                "lastSignature": "signature",
                "newTransactions": "73 transactions after 74",
              },
            },
            "priority": 3,
          },
          {
            "action": "content",
            "expectContentUntil": undefined,
            "header": undefined,
            "id": "id",
            "new": {
              "charlie": {
                "lastSignature": "signature",
                "newTransactions": "73 transactions after 0",
              },
            },
            "priority": 3,
          },
          {
            "action": "content",
            "expectContentUntil": undefined,
            "header": undefined,
            "id": "id",
            "new": {
              "charlie": {
                "lastSignature": "signature",
                "newTransactions": "73 transactions after 73",
              },
            },
            "priority": 3,
          },
          {
            "action": "content",
            "expectContentUntil": undefined,
            "header": undefined,
            "id": "id",
            "new": {
              "alice": {
                "lastSignature": "signature",
                "newTransactions": "54 transactions after 147",
              },
              "bob": {
                "lastSignature": "signature",
                "newTransactions": "1 transactions after 0",
              },
              "charlie": {
                "lastSignature": "signature",
                "newTransactions": "54 transactions after 146",
              },
            },
            "priority": 3,
          },
        ]
      `);
    });
  });
});
