import { CoValueEntry } from "../coValueEntry.js";
import { HYBRID_MESSAGING_MODE } from "../globals.js";
import { PeerEntry, PeerID } from "../peer/PeerEntry.js";
import { AbstractMessageHandler } from "./AbstractMessageHandler.js";
import { AckMessage } from "./types.js";

export type AckMessageHandlerInput = {
  msg: AckMessage;
  peer: PeerEntry;
  entry: CoValueEntry;
};

export class AckResponseHandler extends AbstractMessageHandler {
  constructor(
    private onPushContentAcknowledged?: ({
      entry,
      peerId,
    }: { entry: CoValueEntry; peerId: PeerID }) => void,
  ) {
    super();
  }

  async handleAvailable(input: AckMessageHandlerInput) {
    if (this.onPushContentAcknowledged) {
      this.onPushContentAcknowledged({
        entry: input.entry,
        peerId: input.peer.id,
      });
    }
  }

  async handleLoading(input: AckMessageHandlerInput) {
    if (!HYBRID_MESSAGING_MODE) {
      // When two messaging protocol are stacked, this could produce some excessive "ack" messages
      // out of peer's "known" messages which would lead to this error. Should be ignored in HYBRID_MESSAGING_MODE
      console.error(
        "Unexpected loading state. Ack message is a response to a push request and should not be received for loading coValue.",
        input.msg.id,
        input.peer.id,
      );
    }
  }

  async handleUnknown(input: AckMessageHandlerInput) {
    if (!HYBRID_MESSAGING_MODE) {
      console.error(
        "Unexpected unavailable state. Ack message is a response to a push request and should not be received for unavailable coValue.",
        input.msg.id,
        input.peer.id,
      );
    }
  }
}
