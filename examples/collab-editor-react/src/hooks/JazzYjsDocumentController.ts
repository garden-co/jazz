import { nanoid } from "nanoid";
import * as Y from "yjs";
import type { Db } from "jazz-tools";
import { app, type RoomYjsSnapshot, type RoomYjsUpdate } from "../../schema.js";
import { hashString } from "../lib/hash.js";
import { toUint8Array } from "../lib/yjsUpdate.js";

const snapshotDebounceMs = 2_000;
const snapshotCoalesceMs = 2 * 60 * 1_000;

function latestSnapshot(rows: RoomYjsSnapshot[]): RoomYjsSnapshot | null {
  return rows.reduce<RoomYjsSnapshot | null>(
    (latest, s) => (!latest || s.createdAt.getTime() > latest.createdAt.getTime() ? s : latest),
    null,
  );
}

/**
 * Owns a Y.Doc and keeps it in sync with Jazz for a single room.
 *
 * Subscribes to the room's snapshot and update rows via `db.subscribeAll`,
 * applies them to the Y.Doc (skipping rows we produced), and persists any
 * local edits back to Jazz as new update rows + periodic snapshots. Calls
 * `onReady` once the initial bootstrap has been applied.
 */
export class JazzYjsDocumentController {
  readonly providerInstanceId = nanoid();
  private readonly remoteOrigin = { provider: "jazz" };
  private readonly text: Y.Text;
  private readonly unsubs: Array<() => void> = [];

  private snapshots: RoomYjsSnapshot[] = [];
  private updates: RoomYjsUpdate[] = [];
  private appliedUpdateIds = new Set<string>();
  private didBootstrap = false;
  private isReady = false;
  private snapshotTimeout: ReturnType<typeof setTimeout> | undefined;

  constructor(
    private readonly db: Db,
    readonly roomId: string,
    readonly sessionUserId: string,
    readonly ydoc: Y.Doc,
    private readonly onReady: () => void,
  ) {
    this.text = ydoc.getText("monaco");
    this.ydoc.on("update", this.onYdocUpdate);
    this.text.observe(this.onTextChange);
    this.unsubs.push(
      this.db.subscribeAll(app.roomYjsSnapshots.where({ room_id: roomId }), ({ all }) => {
        this.snapshots = all;
        this.reconcile();
      }),
      this.db.subscribeAll(app.roomYjsUpdates.where({ room_id: roomId }), ({ all }) => {
        this.updates = all;
        this.reconcile();
      }),
    );
  }

  destroy() {
    for (const off of this.unsubs) off();
    this.unsubs.length = 0;
    if (this.snapshotTimeout) clearTimeout(this.snapshotTimeout);
    this.ydoc.off("update", this.onYdocUpdate);
    this.text.unobserve(this.onTextChange);
  }

  /** Apply the latest snapshot once, then any unapplied (non-local) update rows. */
  private reconcile() {
    if (!this.didBootstrap) {
      const snapshot = latestSnapshot(this.snapshots);
      if (snapshot) Y.applyUpdate(this.ydoc, toUint8Array(snapshot.state), this.remoteOrigin);
      this.didBootstrap = true;
    }
    for (const row of this.updates) {
      if (this.appliedUpdateIds.has(row.id)) continue;
      this.appliedUpdateIds.add(row.id);
      if (row.provider_instance_id === this.providerInstanceId) continue;
      Y.applyUpdate(this.ydoc, toUint8Array(row.update), this.remoteOrigin);
    }
    if (!this.isReady) {
      this.isReady = true;
      this.onReady();
    }
  }

  /** Persist each local edit as a new update row. */
  private onYdocUpdate = (update: Uint8Array, origin: unknown) => {
    if (origin === this.remoteOrigin || !this.isReady) return;
    this.db
      .insert(app.roomYjsUpdates, {
        room_id: this.roomId,
        update: new Uint8Array(update),
        session_user_id: this.sessionUserId,
        provider_instance_id: this.providerInstanceId,
        createdAt: new Date(),
      })
      .wait({ tier: "edge" })
      .catch((error: unknown) => {
        console.error("Failed to persist Yjs update", error);
      });
  };

  /** Debounce a snapshot after local text changes settle. */
  private onTextChange = (_event: Y.YTextEvent, transaction: Y.Transaction) => {
    if (transaction.origin === this.remoteOrigin) return;
    if (this.snapshotTimeout) clearTimeout(this.snapshotTimeout);
    this.snapshotTimeout = setTimeout(() => {
      this.saveSnapshot().catch((error: unknown) => {
        console.error("Failed to persist Yjs snapshot", error);
      });
    }, snapshotDebounceMs);
  };

  private async saveSnapshot() {
    if (!this.isReady) return;
    const text = this.text.toString();
    const textHash = await hashString(text);
    const now = new Date();
    const latest = latestSnapshot(this.snapshots);
    if (
      latest?.textHash === textHash &&
      now.getTime() - latest.createdAt.getTime() < snapshotCoalesceMs
    ) {
      return;
    }
    await this.db
      .insert(app.roomYjsSnapshots, {
        room_id: this.roomId,
        state: Y.encodeStateAsUpdate(this.ydoc),
        textHash,
        session_user_id: this.sessionUserId,
        createdAt: now,
      })
      .wait({ tier: "local" });
  }
}
