import { CoValueCore } from "./coValueCore.js";
import { CoValueEntry } from "./coValueEntry.js";
import { RawCoID } from "./ids.js";
import { LocalNode } from "./localNode.js";
import { PeerEntry, PeerID } from "./peer/index.js";
import { AckResponseHandler } from "./sync/AckResponseHandler.js";
import { DataResponseHandler } from "./sync/DataResponseHandler.js";
import { DependencyService } from "./sync/DependencyService.js";
import { LoadService } from "./sync/LoadService.js";
import { PullRequestHandler } from "./sync/PullRequestHandler.js";
import { PushRequestHandler } from "./sync/PushRequestHandler.js";
import { SyncService } from "./sync/SyncService.js";
import {
  CoValueKnownState,
  MessageHandlerInterface,
  SyncMessage,
} from "./sync/types.js";

export type DisconnectedError = "Disconnected";

export type PingTimeoutError = "PingTimeout";

export class SyncManager {
  // "peers" should reside on the local node instance, but it'd be problematic to move it over
  // due to the node cloning and "testWithDifferentAccount" methods on the local node
  // peers = new Peers();
  local: LocalNode;

  requestedSyncs: {
    [id: RawCoID]:
      | { done: Promise<void>; nRequestsThisTick: number }
      | undefined;
  } = {};

  private readonly loadService: LoadService;
  private readonly syncService: SyncService;
  private readonly dependencyService: DependencyService;
  private readonly pullRequestHandler: PullRequestHandler;
  private readonly pushRequestHandler: PushRequestHandler;
  private readonly ackResponseHandler: AckResponseHandler;
  private readonly dataResponseHandler: DataResponseHandler;

  constructor(local: LocalNode) {
    this.local = local;

    this.syncService = new SyncService(
      // onPushContent callback
      ({ entry, peerId }: { entry: CoValueEntry; peerId: PeerID }) => {
        entry.uploadState.setPendingForPeer(peerId);
      },
    );

    this.loadService = new LoadService(this.local.peers);
    this.dependencyService = new DependencyService(this, this.loadService);

    this.pullRequestHandler = new PullRequestHandler(this.loadService);
    this.pushRequestHandler = new PushRequestHandler(
      this.syncService,
      // The reason for this ugly callback here is to avoid having the local node as a dependency in the handler,
      // This should be removed after CoValueCore is decoupled from the local node instance
      this.dependencyService,
      this.local.peers,
    );

    this.ackResponseHandler = new AckResponseHandler(
      // onPushContentAcknowledged callback
      ({ entry, peerId }: { entry: CoValueEntry; peerId: PeerID }) => {
        entry.uploadState.setCompletedForPeer(peerId);
      },
    );

    this.dataResponseHandler = new DataResponseHandler(
      this.dependencyService,
      this.syncService,
      this.local.peers,
    );
  }

  async initialSync(peer: PeerEntry) {
    return this.syncService.initialSync(peer, this.local.coValuesStore);
  }

  async syncCoValue(
    coValue: CoValueCore,
    peersKnownState: CoValueKnownState,
    peers?: PeerEntry[],
  ) {
    if (this.requestedSyncs[coValue.id]) {
      this.requestedSyncs[coValue.id]!.nRequestsThisTick++;
      return this.requestedSyncs[coValue.id]!.done;
    } else {
      const done = new Promise<void>((resolve) => {
        queueMicrotask(async () => {
          delete this.requestedSyncs[coValue.id];
          const entry = this.local.coValuesStore.get(coValue.id);
          await this.syncService.syncCoValue(
            entry,
            peersKnownState,
            peers || this.local.peers.getInPriorityOrder(),
          );
          resolve();
        });
      });

      this.requestedSyncs[coValue.id] = {
        done,
        nRequestsThisTick: 1,
      };
      return done;
    }
  }

  async loadCoValue(id: RawCoID): Promise<CoValueCore | "unavailable"> {
    const entry = this.local.coValuesStore.get(id);
    return this.loadService.loadCoValue(entry);
  }

  handleSyncMessage(msg: SyncMessage, peer: PeerEntry) {
    if (peer.erroredCoValues.has(msg.id)) {
      console.error(
        `Skipping message ${msg.action} on errored coValue ${msg.id} from peer ${peer.id}`,
      );
      return;
    }
    const entry = this.local.coValuesStore.get(msg.id);

    let handler: MessageHandlerInterface;
    switch (msg.action) {
      case "data":
        handler = this.dataResponseHandler;
        break;
      case "push":
        handler = this.pushRequestHandler;
        break;
      case "pull":
        handler = this.pullRequestHandler;
        break;
      case "ack":
        handler = this.ackResponseHandler;
        break;
      default:
        throw new Error(
          `Unknown message type ${(msg as unknown as { action: "string" }).action}`,
        );
    }
    return handler.handle({ msg, peer, entry });
  }

  async waitForUploadIntoPeer(peerId: PeerID, id: RawCoID, timeout: number) {
    const entry = this.local.coValuesStore.get(id);
    if (!entry) {
      throw new Error(`Unknown coValue ${id}`);
    }

    if (entry.uploadState.isCoValueFullyUploadedIntoPeer(peerId)) {
      return true;
    }

    return new Promise(async (resolve, reject) => {
      const timeoutId = setTimeout(() => {
        reject(new Error(`Timeout waiting for sync on ${peerId}/${id}`));
      }, timeout);

      await entry.uploadState.waitForPeer(peerId);
      if (entry.uploadState.isCoValueFullyUploadedIntoPeer(peerId)) {
        resolve(true);
      } else {
        resolve(false);
      }

      clearTimeout(timeoutId);
    });
  }

  async waitForSync(id: RawCoID, timeout = 30_000) {
    const peers = this.local.peers.getAll();

    return Promise.all(
      peers.map((peer) => this.waitForUploadIntoPeer(peer.id, id, timeout)),
    );
  }

  async waitForAllCoValuesSync(timeout = 60_000) {
    const coValues = this.local.coValuesStore.getValues();
    const validCoValues = Array.from(coValues).filter(
      (coValue) =>
        coValue.state.type === "available" || coValue.state.type === "loading",
    );

    return Promise.all(
      validCoValues.map((coValue) => this.waitForSync(coValue.id, timeout)),
    );
  }
}
