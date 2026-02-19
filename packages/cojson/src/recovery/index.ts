import { LocalNode } from "../localNode.js";
import { logger } from "../logger.js";
import {
  normalizeAuthoritativeSessionContent,
  type NormalizedAuthoritativeSession,
} from "./normalizeAuthoritativeSessionContent.js";
import type {
  NewContentMessage,
  SignatureMismatchErrorMessage,
} from "../sync.js";
import {
  AvailableCoValueCore,
  VerifiedTransaction,
} from "../coValueCore/coValueCore.js";
import { decryptTransactionChangesAndMeta } from "../coValueCore/decryptTransactionChangesAndMeta.js";
import { CO_VALUE_PRIORITY } from "../priority.js";
import { findCommonPrefixLength, isCurrentNodeSessionOwner } from "./utils.js";
import { JsonValue } from "../jsonValue.js";

type OwnerRecoveryContext = {
  local: LocalNode;
  msg: SignatureMismatchErrorMessage;
  coValue: AvailableCoValueCore;
  tempNode: LocalNode;
  contentWithoutSession: NewContentMessage[];
  normalized: NormalizedAuthoritativeSession;
};

type SessionConflictRecoveryErrorCode =
  | "MISSING_DEPENDENCIES_UNAVAILABLE"
  | "MISSING_DEPENDENCIES_AFTER_REBUILD"
  | "REPLAY_MISSING_PARSED_CHANGES"
  | "REPLAY_INVALID_METADATA";

class SessionConflictRecoveryError extends Error {
  constructor(
    readonly code: SessionConflictRecoveryErrorCode,
    message: string,
  ) {
    super(message);
    this.name = "SessionConflictRecoveryError";
  }
}

export function recoverSignatureMismatch(
  local: LocalNode,
  msg: SignatureMismatchErrorMessage,
): void {
  if (!isCurrentNodeSessionOwner(local, msg.sessionID)) {
    logger.info("Skipping non-owner SignatureMismatch recovery for now", {
      id: msg.id,
      sessionID: msg.sessionID,
    });
    return;
  }

  if (!local.storage) {
    logger.warn("Skipping SignatureMismatch recovery due to missing storage", {
      id: msg.id,
      sessionID: msg.sessionID,
    });
    return;
  }

  const normalized = normalizeAuthoritativeSessionContent(msg.content);

  if (!normalized.ok) {
    logger.warn("Skipping SignatureMismatch recovery due to invalid content", {
      id: msg.id,
      sessionID: msg.sessionID,
    });
    return;
  }

  runOwnerRecovery(local, msg, normalized.value).catch((err) => {
    logger.error("Failed to run owner SignatureMismatch recovery", {
      id: msg.id,
      sessionID: msg.sessionID,
      err,
    });
  });
}

async function runOwnerRecovery(
  local: LocalNode,
  msg: SignatureMismatchErrorMessage,
  normalized: NormalizedAuthoritativeSession,
): Promise<void> {
  const coValue = local.getCoValue(msg.id);

  if (!coValue.isAvailable()) {
    logger.warn(
      "Skipping owner SignatureMismatch recovery for unavailable CoValue",
      {
        id: msg.id,
        sessionID: msg.sessionID,
      },
    );
    return;
  }

  // We play the rebase in a temporary node because:
  // 1. In case the rebase fails, we can rollback the changes without data loss
  // 2. This way we can update the storage with a single atomic operation
  const context = await buildTempRebasedNode(local, msg, coValue, normalized);

  await resolveAndRebuildIfDependenciesMissing(context);

  const rebasedCoValue = context.tempNode.getCoValue(msg.id);

  if (!rebasedCoValue.isAvailable()) {
    logger.warn(
      "Skipping owner SignatureMismatch recovery due to missing verified content",
      {
        id: msg.id,
        sessionID: msg.sessionID,
      },
    );
    return;
  }

  const replayedTailTransactions = await replayLocalTailTransactions(
    context,
    rebasedCoValue,
  );
  persistAndSwapRecoveredState(context, rebasedCoValue);

  logger.info("Completed owner SignatureMismatch recovery", {
    id: msg.id,
    sessionID: msg.sessionID,
    replayedTailTransactions,
  });
}

async function buildTempRebasedNode(
  local: LocalNode,
  msg: SignatureMismatchErrorMessage,
  coValue: AvailableCoValueCore,
  normalized: NormalizedAuthoritativeSession,
): Promise<OwnerRecoveryContext> {
  const { node: tempNode } = await local.loadCoValueAsDifferentAgent(
    msg.id,
    local.agentSecret,
    local.getCurrentAccountOrAgentID(),
    msg.sessionID,
  );
  // Rebase in an isolated node from empty state so stale local parsing
  // cannot leak into the recovered authoritative history.
  tempNode.internalDeleteCoValue(msg.id);

  const knownState = coValue.knownState();
  const contentWithoutSession =
    coValue.newContentSince({
      id: msg.id,
      header: false,
      sessions: {
        [msg.sessionID]: knownState.sessions[msg.sessionID] ?? 0,
      },
    }) ?? [];

  applyRecoveryInputToTempNode(
    tempNode,
    msg,
    contentWithoutSession,
    normalized,
  );

  return {
    local,
    msg,
    coValue,
    tempNode,
    contentWithoutSession,
    normalized,
  };
}

async function resolveAndRebuildIfDependenciesMissing(
  context: OwnerRecoveryContext,
): Promise<void> {
  const missingDependencies = context.tempNode.getCoValue(
    context.msg.id,
  ).missingDependencies;

  if (missingDependencies.size === 0) {
    return;
  }

  // After loading dependencies we must rebuild from scratch, otherwise
  // the temporary value may still contain parse decisions from missing deps.
  context.tempNode.internalDeleteCoValue(context.msg.id);

  const peerState = context.local.connectToNodeAsServer(
    context.tempNode,
    context.msg.id.toString(),
  );
  const missingDeps = await Promise.all(
    Array.from(missingDependencies).map((dep) =>
      context.local.loadCoValueCore(dep),
    ),
  );
  peerState.gracefulShutdown();

  if (missingDeps.some((dep) => !dep.isAvailable())) {
    throw new SessionConflictRecoveryError(
      "MISSING_DEPENDENCIES_UNAVAILABLE",
      "Some dependencies are not available",
    );
  }

  applyRecoveryInputToTempNode(
    context.tempNode,
    context.msg,
    context.contentWithoutSession,
    context.normalized,
  );

  if (
    context.tempNode.getCoValue(context.msg.id).missingDependencies.size > 0
  ) {
    throw new SessionConflictRecoveryError(
      "MISSING_DEPENDENCIES_AFTER_REBUILD",
      "Some dependencies are not available after rebuild",
    );
  }
}

async function replayLocalTailTransactions(
  context: OwnerRecoveryContext,
  rebasedCoValue: ReturnType<LocalNode["getCoValue"]>,
): Promise<number> {
  const expectedTxs = context.normalized.transactions.length;

  // Replay must start only after the authoritative session is fully applied;
  // otherwise we can replay local tail against an incomplete base.
  await rebasedCoValue.waitForAsync(
    (loadedCoValue) =>
      loadedCoValue.knownState().sessions[context.msg.sessionID] ===
      expectedTxs,
  );

  const localBeforeSession = context.coValue.verified.getSession(
    context.msg.sessionID,
  );
  const localBeforeTransactions = localBeforeSession?.transactions ?? [];
  const commonPrefix = findCommonPrefixLength(
    localBeforeTransactions,
    context.normalized.transactions,
  );

  const localTail = localBeforeTransactions.slice(commonPrefix).map((tx, i) => {
    const txIndex = i + commonPrefix;
    const transaction = new VerifiedTransaction(
      context.msg.id,
      context.msg.sessionID,
      txIndex,
      tx,
      undefined,
      undefined,
      undefined,
      () => {},
    );

    if (tx.privacy === "private") {
      decryptTransactionChangesAndMeta(context.coValue, transaction);
    }

    if (!transaction.changes) {
      throw new SessionConflictRecoveryError(
        "REPLAY_MISSING_PARSED_CHANGES",
        `Unable to replay transaction ${txIndex}: missing parsed changes`,
      );
    }

    if (tx.meta && !transaction.meta) {
      throw new SessionConflictRecoveryError(
        "REPLAY_INVALID_METADATA",
        `Unable to replay transaction ${txIndex}: invalid transaction metadata`,
      );
    }

    return transaction as VerifiedTransaction & { changes: JsonValue[] };
  });

  for (const tx of localTail) {
    rebasedCoValue.makeTransaction(
      tx.changes,
      tx.tx.privacy,
      tx.meta,
      tx.madeAt,
    );
  }

  return localTail.length;
}

function persistAndSwapRecoveredState(
  context: OwnerRecoveryContext,
  rebasedCoValue: AvailableCoValueCore,
): void {
  const rebasedSession = rebasedCoValue.verified.getFullSessionContent(
    context.msg.sessionID,
  );

  context.local.storage?.store(
    {
      action: "replaceSessionHistory",
      coValueId: context.msg.id,
      sessionID: context.msg.sessionID,
      content: rebasedSession,
    },
    () => undefined,
  );

  context.coValue.replaceVerifiedContent(rebasedCoValue.verified);
}

function applyRecoveryInputToTempNode(
  tempNode: LocalNode,
  msg: SignatureMismatchErrorMessage,
  contentWithoutSession: NewContentMessage[],
  normalized: NormalizedAuthoritativeSession,
): void {
  for (const content of contentWithoutSession) {
    tempNode.syncManager.handleNewContent(content, "storage");
  }

  for (const content of normalized.content) {
    tempNode.syncManager.handleNewContent(
      {
        action: "content",
        id: msg.id,
        new: {
          [msg.sessionID]: content,
        },
        priority: CO_VALUE_PRIORITY.HIGH,
      },
      "storage",
    );
  }
}
