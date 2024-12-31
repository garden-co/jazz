import { CoValueHeader, SessionNewContent } from "../coValueCore.js";
import { CoValueEntry } from "../coValueEntry.js";
import { RawCoID, SessionID } from "../ids.js";
import { PeerEntry } from "../peer/PeerEntry.js";
import { CoValuePriority } from "../priority.js";

export type CoValueKnownState = {
  id: RawCoID;
  // Is coValue known by peer
  header: boolean;
  // Number of known sessions
  sessions: { [sessionID: SessionID]: number };
};

export function emptyKnownState(id: RawCoID): CoValueKnownState {
  return {
    id,
    header: false,
    sessions: {},
  };
}

export type SyncMessage =
  | LoadMessage
  | KnownStateMessage
  | NewContentMessage
  | PullMessage
  | PushMessage
  | AckMessage
  | DataMessage;

export type LoadMessage = {
  action: "load";
} & CoValueKnownState;

export type PullMessage = {
  action: "pull";
} & CoValueKnownState;

export type KnownStateMessage = {
  action: "known";
  asDependencyOf?: RawCoID;
  isCorrection?: boolean;
} & CoValueKnownState;

export type AckMessage = {
  action: "ack";
} & CoValueKnownState;

export type CoValueContent = {
  id: RawCoID;
  header?: CoValueHeader;
  priority: CoValuePriority;
  new: {
    [sessionID: SessionID]: SessionNewContent;
  };
};

export type NewContentMessage = {
  action: "content";
} & CoValueContent;

export type DataMessage = {
  known: boolean;
  action: "data";
  asDependencyOf?: RawCoID;
} & CoValueContent;

export type PushMessage = {
  action: "push";
  asDependencyOf?: RawCoID;
} & CoValueContent;

export type MessageHandlerInput = {
  msg: SyncMessage;
  peer: PeerEntry;
  entry: CoValueEntry;
};

export type PushMessageHandlerInput = {
  msg: PushMessage;
  peer: PeerEntry;
  entry: CoValueEntry;
};

export type DataMessageHandlerInput = {
  msg: DataMessage;
  peer: PeerEntry;
  entry: CoValueEntry;
};

export interface MessageHandlerInterface {
  handle({ msg, peer, entry }: MessageHandlerInput): Promise<unknown>;
}

export abstract class BaseMessageHandler implements MessageHandlerInterface {
  handle({ msg, peer, entry }: MessageHandlerInput): Promise<unknown> {
    // we don't await for handlers to be resolved to not block the execution
    switch (entry.state.type) {
      case "available":
        return this.handleAvailable({ msg, peer, entry });
      case "loading":
        return this.handleLoading({ msg, peer, entry });
      case "unknown":
      case "unavailable":
        return this.handleUnknown({ msg, peer, entry });
    }
  }

  abstract handleAvailable(input: MessageHandlerInput): Promise<unknown>;
  abstract handleLoading(input: MessageHandlerInput): Promise<unknown>;
  abstract handleUnknown(input: MessageHandlerInput): Promise<unknown>;
}
