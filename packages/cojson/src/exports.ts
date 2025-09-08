import { base64URLtoBytes, bytesToBase64url } from "./base64url.js";
import { type RawCoValue } from "./coValue.js";
import {
  CoValueCore,
  idforHeader,
  type AvailableCoValueCore,
} from "./coValueCore/coValueCore.js";
import { CoValueUniqueness } from "./coValueCore/verifiedState.js";
import {
  ControlledAccount,
  ControlledAgent,
  RawAccount,
  RawProfile,
  accountHeaderForInitialAgentSecret,
} from "./coValues/account.js";
import { OpID, RawCoList } from "./coValues/coList.js";
import { RawCoMap } from "./coValues/coMap.js";
import { RawCoPlainText, stringifyOpID } from "./coValues/coPlainText.js";
import {
  BinaryStreamItem,
  BinaryStreamStart,
  CoStreamItem,
  RawBinaryCoStream,
  RawCoStream,
} from "./coValues/coStream.js";
import { EVERYONE, RawGroup } from "./coValues/group.js";
import type { Everyone } from "./coValues/group.js";
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
import { LocalNode } from "./localNode.js";
import type { AccountRole, Role } from "./permissions.js";
import { ConnectedPeerChannel, connectedPeers } from "./streamUtils.js";
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
import { AgentSecret, textDecoder, textEncoder } from "./crypto/crypto.js";
import type { AgentID, RawCoID, SessionID } from "./ids.js";
import type { JsonObject, JsonValue } from "./jsonValue.js";
import type * as Media from "./media.js";
import { disablePermissionErrors } from "./permissions.js";
import type { Peer, SyncMessage } from "./sync.js";
import {
  DisconnectedError,
  SyncManager,
  emptyKnownState,
  hwrServerPeerSelector,
} from "./sync.js";

import {
  getContentMessageSize,
  getTransactionSize,
} from "./coValueContentMessage.js";
import { getDependedOnCoValuesFromRawData } from "./coValueCore/utils.js";
import {
  CO_VALUE_LOADING_CONFIG,
  TRANSACTION_CONFIG,
  setCoValueLoadingRetryDelay,
  setIncomingMessagesTimeBudget,
  setMaxRecommendedTxSize,
} from "./config.js";
import { LogLevel, logger } from "./logger.js";
import { CO_VALUE_PRIORITY, getPriorityFromHeader } from "./priority.js";
import { getDependedOnCoValues } from "./storage/syncUtils.js";

type Value = JsonValue | AnyRawCoValue;

export { PriorityBasedMessageQueue } from "./queue/PriorityBasedMessageQueue.js";
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
  getDependedOnCoValues,
  getDependedOnCoValuesFromRawData,
  accountOrAgentIDfromSessionID,
  isAccountID,
  accountHeaderForInitialAgentSecret,
  idforHeader,
  StreamingHash,
  getPriorityFromHeader,
  getGroupDependentKeyList,
  getGroupDependentKey,
  disablePermissionErrors,
  SyncManager,
  CO_VALUE_LOADING_CONFIG,
  CO_VALUE_PRIORITY,
  setIncomingMessagesTimeBudget,
  setCoValueLoadingRetryDelay,
  ConnectedPeerChannel,
  textEncoder,
  textDecoder,
  getTransactionSize,
  getContentMessageSize,
  TRANSACTION_CONFIG,
  setMaxRecommendedTxSize,
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
  ControlledAccount,
  AccountMeta,
  RawAccountMigration,
  RawProfile as Profile,
  SessionID,
  Media,
  CoValueCore,
  ControlledAgent,
  JsonObject,
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
  emptyKnownState,
  RawCoPlainText,
  stringifyOpID,
  logger,
  LogLevel,
  base64URLtoBytes,
  bytesToBase64url,
  hwrServerPeerSelector,
};

export type {
  Value,
  DisconnectedError,
  CoValueUniqueness,
  Stringified,
  CoStreamItem,
  BinaryStreamItem,
  BinaryStreamStart,
  OpID,
  AccountRole,
  AvailableCoValueCore,
};

export * from "./storage/index.js";

// biome-ignore format: off
// eslint-disable-next-line @typescript-eslint/no-namespace
export namespace CojsonInternalTypes {
  export type CoValueKnownState = import("./sync.js").CoValueKnownState;
  export type CoJsonValue<T> = import("./jsonValue.js").CoJsonValue<T>;
  export type DoneMessage = import("./sync.js").DoneMessage;
  export type Encrypted<T extends JsonValue, N extends JsonValue> = import("./crypto/crypto.js").Encrypted<T, N>;
  export type KeySecret = import("./crypto/crypto.js").KeySecret;
  export type KnownStateMessage = import("./sync.js").KnownStateMessage;
  export type LoadMessage = import("./sync.js").LoadMessage;
  export type NewContentMessage = import("./sync.js").NewContentMessage;
  export type SessionNewContent = import("./sync.js").SessionNewContent;
  export type CoValueHeader = import("./coValueCore/verifiedState.js").CoValueHeader;
  export type Transaction = import("./coValueCore/verifiedState.js").Transaction;
  export type TransactionID = import("./ids.js").TransactionID;
  export type Signature = import("./crypto/crypto.js").Signature;
  export type RawCoID = import("./ids.js").RawCoID;
  export type ProfileShape = import("./coValues/account.js").ProfileShape;
  export type SealerSecret = import("./crypto/crypto.js").SealerSecret;
  export type SignerID = import("./crypto/crypto.js").SignerID;
  export type SignerSecret = import("./crypto/crypto.js").SignerSecret;
  export type JsonObject = import("./jsonValue.js").JsonObject;
  export type OutgoingPeerChannel = import("./sync.js").OutgoingPeerChannel;
  export type IncomingPeerChannel = import("./sync.js").IncomingPeerChannel;
}
// biome-ignore format: on
