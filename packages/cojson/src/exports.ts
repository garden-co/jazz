import { base64URLtoBytes, bytesToBase64url } from "./base64url.js";
import { type RawCoValue } from "./coValue.js";
import {
  CoValueCore,
  type CoValueUniqueness,
  MAX_RECOMMENDED_TX_SIZE,
  idforHeader,
} from "./coValueCore.js";
import {
  ControlledAgent,
  RawAccount,
  RawControlledAccount,
  RawProfile,
  accountHeaderForInitialAgentSecret,
} from "./coValues/account.js";
import { OpID, RawCoList } from "./coValues/coList.js";
import { RawCoMap } from "./coValues/coMap.js";
import { RawCoPlainText, stringifyOpID } from "./coValues/coPlainText.js";
import {
  CoStreamItem,
  RawBinaryCoStream,
  RawCoStream,
} from "./coValues/coStream.js";
import type { Everyone } from "./coValues/group.js";
import { EVERYONE, RawGroup } from "./coValues/group.js";
import {
  CryptoProvider,
  StreamingHash,
  secretSeedLength,
  shortHashLength,
} from "./crypto/crypto.js";
import {
  getGroupDependentKey,
  getGroupDependentKeyList,
  isRawCoID,
  rawCoIDfromBytes,
  rawCoIDtoBytes,
} from "./ids.js";
import { Stringified, parseJSON, stableStringify } from "./jsonStringify.js";
import {
  IncomingSyncStream,
  LocalNode,
  OutgoingSyncQueue,
} from "./localNode.js";
import { emptyDataMessage, unknownDataMessage } from "./peer/PeerOperations.js";
import type { Role } from "./permissions.js";
import { getPriorityFromHeader } from "./priority.js";
import { FileSystem } from "./storage/FileSystem.js";
import { BlockFilename, LSMStorage, WalFilename } from "./storage/index.js";
import { Channel, connectedPeers } from "./streamUtils.js";
import { DisconnectedError, PingTimeoutError } from "./sync.js";
import type { SyncMessage } from "./sync/types.js";
import { emptyKnownState } from "./sync/types.js";
import { accountOrAgentIDfromSessionID } from "./typeUtils/accountOrAgentIDfromSessionID.js";
import { expectGroup } from "./typeUtils/expectGroup.js";
import { isAccountID } from "./typeUtils/isAccountID.js";

import type { AnyRawCoValue, CoID } from "./coValue.js";
import type {
  AccountMeta,
  RawAccountID,
  RawAccountMigration,
} from "./coValues/account.js";
import type {
  BinaryCoStreamMeta,
  BinaryStreamInfo,
} from "./coValues/coStream.js";
import type { InviteSecret } from "./coValues/group.js";
import type { AgentSecret } from "./crypto/crypto.js";
import type { AgentID, RawCoID, SessionID } from "./ids.js";
import type { JsonValue } from "./jsonValue.js";
import type * as Media from "./media.js";
import type { Peer } from "./peer/index.js";
import { disablePermissionErrors } from "./permissions.js";
type Value = JsonValue | AnyRawCoValue;

/** @hidden */
export const cojsonInternals = {
  connectedPeers,
  rawCoIDtoBytes,
  rawCoIDfromBytes,
  secretSeedLength,
  shortHashLength,
  expectGroup,
  base64URLtoBytes,
  bytesToBase64url,
  parseJSON,
  stableStringify,
  accountOrAgentIDfromSessionID,
  isAccountID,
  accountHeaderForInitialAgentSecret,
  idforHeader,
  StreamingHash,
  Channel,
  getPriorityFromHeader,
  getGroupDependentKeyList,
  getGroupDependentKey,
  disablePermissionErrors,
};

export {
  LocalNode,
  RawGroup,
  Role,
  EVERYONE,
  Everyone,
  RawCoMap,
  RawCoList,
  RawCoStream,
  RawBinaryCoStream,
  RawCoValue,
  RawCoID,
  CoID,
  AnyRawCoValue,
  RawAccount,
  RawAccountID,
  AccountMeta,
  RawAccountMigration,
  RawProfile as Profile,
  SessionID,
  Media,
  CoValueCore,
  ControlledAgent,
  RawControlledAccount,
  MAX_RECOMMENDED_TX_SIZE,
  JsonValue,
  Peer,
  BinaryStreamInfo,
  BinaryCoStreamMeta,
  AgentID,
  AgentSecret,
  InviteSecret,
  CryptoProvider,
  SyncMessage,
  isRawCoID,
  LSMStorage,
  emptyKnownState,
  emptyDataMessage,
  unknownDataMessage,
  RawCoPlainText,
  stringifyOpID,
};

export type {
  Value,
  FileSystem,
  BlockFilename,
  WalFilename,
  IncomingSyncStream,
  OutgoingSyncQueue,
  DisconnectedError,
  PingTimeoutError,
  CoValueUniqueness,
  Stringified,
  CoStreamItem,
  OpID,
};

// eslint-disable-next-line @typescript-eslint/no-namespace
export namespace CojsonInternalTypes {
  export type KnownStateMessage = import("./sync/types.js").KnownStateMessage;
  export type CoValueKnownState = import("./sync/types.js").CoValueKnownState;
  export type CoValueContent = import("./sync/types.js").CoValueContent;
  export type NewContentMessage = import("./sync/types.js").NewContentMessage;
  export type PullMessage = import("./sync/types.js").PullMessage;
  export type PushMessage = import("./sync/types.js").PushMessage;
  export type DataMessage = import("./sync/types.js").DataMessage;
  export type AckMessage = import("./sync/types.js").AckMessage;
  export type SessionNewContent = import("./coValueCore.js").SessionNewContent;
  export type CoValueHeader = import("./coValueCore.js").CoValueHeader;
  export type Transaction = import("./coValueCore.js").Transaction;
  export type TransactionID = import("./ids.js").TransactionID;
  export type Signature = import("./crypto/crypto.js").Signature;
  export type RawCoID = import("./ids.js").RawCoID;
  export type ProfileShape = import("./coValues/account.js").ProfileShape;
  export type SealerSecret = import("./crypto/crypto.js").SealerSecret;
  export type SignerID = import("./crypto/crypto.js").SignerID;
  export type SignerSecret = import("./crypto/crypto.js").SignerSecret;
  export type JsonObject = import("./jsonValue.js").JsonObject;
}
