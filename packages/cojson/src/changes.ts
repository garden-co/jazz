import { isRawCoID, type RawCoID, type SessionID } from "./ids.js";
import type { LocalNode } from "./localNode.js";
import type { CoValueCore } from "./coValueCore/coValueCore.js";
import { areCurrentSessionsInSyncWith } from "./knownState.js";
import type { RawCoValue } from "./coValue.js";
import { expectMap, expectList, expectStream } from "./coValue.js";

export type ChangesMessage = {
  added: Set<RawCoID>;
  removed: Set<RawCoID>;
  changed: Set<RawCoID>;
};

export type ChangesCursor = {
  rootId: RawCoID;
  frontiers: Record<RawCoID, Record<SessionID, number>>;
};

export type ChangesSubscription = {
  unsubscribe(): void;
  cursor(): ChangesCursor | undefined;
};

export class CursorError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "CursorError";
  }
}

/**
 * Extracts direct ref IDs from a loaded CoValue's content.
 * Works for CoMap (ref values), CoList (ref items), and CoStream/CoFeed (ref entries).
 */
export function extractDirectRefIds(content: RawCoValue): Set<RawCoID> {
  const refs = new Set<RawCoID>();

  switch (content.type) {
    case "comap": {
      const map = expectMap(content);
      for (const key of map.keys()) {
        const value = map.get(key);
        if (typeof value === "string" && isRawCoID(value)) {
          refs.add(value);
        }
      }
      break;
    }
    case "colist": {
      const list = expectList(content);
      for (const item of list.asArray()) {
        if (typeof item === "string" && isRawCoID(item)) {
          refs.add(item);
        }
      }
      break;
    }
    case "costream": {
      const stream = expectStream(content);
      for (const sessionId of stream.sessions()) {
        const item = stream.lastItemIn(sessionId);
        if (item && typeof item.value === "string" && isRawCoID(item.value)) {
          refs.add(item.value);
        }
      }
      break;
    }
  }

  return refs;
}

function frontiersChanged(
  a: Record<string, number>,
  b: Record<string, number>,
): boolean {
  return (
    !areCurrentSessionsInSyncWith(a, b) || !areCurrentSessionsInSyncWith(b, a)
  );
}

/**
 * Loads the current frontier for a root CoValue and its direct refs.
 */
export async function loadChangesCursor(
  node: LocalNode,
  rootId: RawCoID,
): Promise<ChangesCursor> {
  const rootCore = await node.loadCoValueCore(rootId);

  if (!rootCore.isAvailable()) {
    throw new Error(`Root CoValue ${rootId} is unavailable`);
  }

  const frontiers: Record<RawCoID, Record<SessionID, number>> = {};
  frontiers[rootId] = { ...rootCore.knownState().sessions };

  const content = rootCore.getCurrentContent();
  const refIds = extractDirectRefIds(content);

  const loadPromises: Promise<void>[] = [];
  for (const refId of refIds) {
    loadPromises.push(
      node.loadCoValueCore(refId).then((refCore) => {
        if (refCore.isAvailable()) {
          frontiers[refId] = { ...refCore.knownState().sessions };
        }
      }),
    );
  }

  await Promise.all(loadPromises);

  return { rootId, frontiers };
}

/**
 * Subscribes to changes on a root CoValue and its direct refs,
 * resuming from a cursor. Emits ChangesMessage diffs.
 */
export function subscribeToChanges(
  node: LocalNode,
  rootId: RawCoID,
  cursor: ChangesCursor,
  listener: (changes: ChangesMessage, cursor: ChangesCursor) => void,
): ChangesSubscription {
  if (cursor.rootId !== rootId) {
    throw new CursorError(
      `Cursor rootId ${cursor.rootId} does not match ${rootId}`,
    );
  }

  let lastCursor: ChangesCursor | undefined;
  let unsubscribed = false;
  let rootUnsub: (() => void) | undefined;
  let currentRefIds = new Set<RawCoID>();
  const refUnsubscribers = new Map<RawCoID, () => void>();
  const changedIds = new Set<RawCoID>();
  let pendingEmit = false;

  function scheduleEmit() {
    if (pendingEmit || unsubscribed) return;
    pendingEmit = true;
    queueMicrotask(() => {
      pendingEmit = false;
      if (unsubscribed) return;
      emitDiff();
    });
  }

  function buildCurrentCursor(
    rootCore: CoValueCore,
    refIds: Set<RawCoID>,
  ): ChangesCursor {
    const frontiers: Record<RawCoID, Record<SessionID, number>> = {};
    frontiers[rootId] = { ...rootCore.knownState().sessions };

    for (const refId of refIds) {
      const refCore = node.getCoValue(refId);
      if (refCore.isAvailable()) {
        frontiers[refId] = { ...refCore.knownState().sessions };
      }
    }

    return { rootId, frontiers };
  }

  function emitDiff() {
    if (unsubscribed) return;

    const rootCore = node.getCoValue(rootId);
    if (!rootCore.isAvailable()) return;

    const content = rootCore.getCurrentContent();
    const newRefIds = extractDirectRefIds(content);

    const added = new Set<RawCoID>();
    const removed = new Set<RawCoID>();
    const changed = new Set<RawCoID>();

    for (const id of newRefIds) {
      if (!currentRefIds.has(id)) {
        added.add(id);
      }
    }
    for (const id of currentRefIds) {
      if (!newRefIds.has(id)) {
        removed.add(id);
        const unsub = refUnsubscribers.get(id);
        if (unsub) {
          unsub();
          refUnsubscribers.delete(id);
        }
      }
    }

    // Use the most recent frontier for comparisons, advancing after each emission
    const activeFrontiers = lastCursor?.frontiers ?? cursor.frontiers;

    const rootFrontier = activeFrontiers[rootId];
    if (rootFrontier) {
      if (frontiersChanged(rootFrontier, rootCore.knownState().sessions)) {
        changed.add(rootId);
      }
    }

    for (const id of changedIds) {
      changed.add(id);
    }
    changedIds.clear();

    for (const id of newRefIds) {
      if (!added.has(id) && !changed.has(id)) {
        const savedFrontier = activeFrontiers[id];
        if (savedFrontier) {
          const refCore = node.getCoValue(id);
          if (refCore.isAvailable()) {
            if (
              frontiersChanged(savedFrontier, refCore.knownState().sessions)
            ) {
              changed.add(id);
            }
          }
        }
      }
    }

    if (added.size === 0 && removed.size === 0 && changed.size === 0) {
      currentRefIds = newRefIds;
      subscribeToRefs(newRefIds);
      return;
    }

    const newCursor = buildCurrentCursor(rootCore, newRefIds);
    lastCursor = newCursor;
    currentRefIds = newRefIds;
    subscribeToRefs(newRefIds);

    listener({ added, removed, changed }, newCursor);
  }

  function subscribeToRefs(refIds: Set<RawCoID>) {
    for (const refId of refIds) {
      if (refUnsubscribers.has(refId)) continue;
      subscribeToRef(refId);
    }
  }

  function subscribeToRef(refId: RawCoID) {
    const refCore = node.getCoValue(refId);

    sendKnownMessage(node, refId, cursor.frontiers[refId]);

    const unsub = refCore.subscribe((core) => {
      if (unsubscribed) return;
      if (core.isAvailable()) {
        changedIds.add(refId);
        scheduleEmit();
      }
    }, false);

    refUnsubscribers.set(refId, unsub);
  }

  function start() {
    sendKnownMessage(node, rootId, cursor.frontiers[rootId]);

    const cursorIds = Object.keys(cursor.frontiers) as RawCoID[];
    for (const id of cursorIds) {
      if (id === rootId) continue;
      currentRefIds.add(id);
      subscribeToRef(id);
    }

    const rootCore = node.getCoValue(rootId);
    rootUnsub = rootCore.subscribe((core) => {
      if (unsubscribed) return;
      if (core.isAvailable()) {
        scheduleEmit();
      }
    });
  }

  start();

  return {
    unsubscribe() {
      unsubscribed = true;
      rootUnsub?.();
      for (const unsub of refUnsubscribers.values()) {
        unsub();
      }
      refUnsubscribers.clear();
    },
    cursor() {
      return lastCursor;
    },
  };
}

function sendKnownMessage(
  node: LocalNode,
  id: RawCoID,
  frontier: Record<SessionID, number> | undefined,
) {
  const peers = node.syncManager.getServerPeers(id);
  const sessions = frontier ? ({ ...frontier } as Record<string, number>) : {};

  for (const peer of peers) {
    node.syncManager.trySendToPeer(peer, {
      action: "known",
      id,
      header: !!frontier,
      sessions,
    });
  }
}
