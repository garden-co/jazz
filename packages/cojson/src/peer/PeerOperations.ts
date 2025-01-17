import { CoValueCore } from "../coValueCore.js";
import { RawCoID } from "../ids.js";
import {
  CoValueKnownState,
  DataMessage,
  PushMessage,
  SyncMessage,
} from "../sync/types.js";
import { PeerEntry } from "./PeerEntry.js";

export function emptyDataMessage(
  id: RawCoID,
  asDependencyOf?: RawCoID,
): DataMessage {
  const message: DataMessage = {
    id,
    known: true,
    header: undefined,
    action: "data",
    priority: 0,
    new: {},
  };
  return asDependencyOf ? { ...message, asDependencyOf } : message;
}

export function unknownDataMessage(
  id: RawCoID,
  asDependencyOf?: RawCoID,
): DataMessage {
  const message: DataMessage = {
    id,
    known: false,
    header: undefined,
    action: "data",
    priority: 0,
    new: {},
  };

  return asDependencyOf ? { ...message, asDependencyOf } : message;
}

/**
 * The PeerOperations class centralizes the sending logic for the atomic synchronization operations:
 * pull, push, ack, and data, implementing the protocol.
 */
export class PeerOperations {
  constructor(private readonly peer: PeerEntry) {}

  async pull({ knownState }: { knownState: CoValueKnownState }) {
    if (this.peer.closed) return;

    return this.peer.pushOutgoingMessage({
      ...knownState,
      action: "pull",
    });
  }

  async ack({ knownState }: { knownState: CoValueKnownState }) {
    if (this.peer.closed) return;

    return this.peer.pushOutgoingMessage({
      ...knownState,
      action: "ack",
    });
  }

  push({
    peerKnownState,
    coValue,
  }: { peerKnownState: CoValueKnownState; coValue: CoValueCore }) {
    if (this.peer.closed) return;

    return this.sendContent({
      peerKnownState,
      coValue,
      action: "push",
    });
  }

  async data({
    peerKnownState,
    coValue,
    dependencies = [],
  }: {
    peerKnownState: CoValueKnownState;
    coValue: CoValueCore | "empty" | "unknown";
    dependencies?: CoValueCore[];
  }) {
    if (this.peer.closed) return;

    if (coValue === "empty") {
      return this.peer.pushOutgoingMessage(emptyDataMessage(peerKnownState.id));
    }
    if (coValue === "unknown") {
      return this.peer.pushOutgoingMessage(
        unknownDataMessage(peerKnownState.id),
      );
    }

    const sendContentOrEmptyMessage = async (params: SendContentParamsType) => {
      const sentContentPiecesNumber = await this.sendContent(params);
      if (!sentContentPiecesNumber) {
        void this.data({ peerKnownState, coValue: "empty" });
      }
    };

    // send dependencies first
    await Promise.all(
      dependencies.map((depCoValue) =>
        sendContentOrEmptyMessage({
          peerKnownState,
          coValue: depCoValue,
          action: "data",
          asDependencyOf: coValue.id,
        }),
      ),
    );

    // Send new content pieces (possibly, in chunks) created after peerKnownState that passed in
    return sendContentOrEmptyMessage({
      peerKnownState,
      coValue,
      action: "data",
    });
  }

  private async sendContent({
    peerKnownState,
    coValue,
    action,
    asDependencyOf,
  }: SendContentParamsType): Promise<number> {
    const newContentPieces = coValue.newContentSince(peerKnownState);

    if (newContentPieces) {
      for (const [_i, piece] of newContentPieces.entries()) {
        let msg: SyncMessage;
        if (action === "data") {
          msg = { ...piece, action, known: true } as DataMessage;
        } else {
          msg = { ...piece, action } as PushMessage;
        }

        if (asDependencyOf) msg = { ...msg, asDependencyOf };

        void this.peer.pushOutgoingMessage(msg);
      }
    }

    return newContentPieces?.length || 0;
  }
}

type SendContentParamsType = {
  peerKnownState: CoValueKnownState;
  coValue: CoValueCore;
  action: "push" | "data";
  asDependencyOf?: RawCoID;
};
