import { CoValueCore, LocalNode, RawControlledAccount } from "../exports";
import {
  CoValueKnownState,
  NewContentMessage,
  Peer,
  SyncMessage,
} from "../sync";

function simplifySessions(msg: CoValueKnownState) {
  const count = Object.values(msg.sessions).reduce(
    (acc, session) => acc + session,
    0,
  );

  if (msg.header) {
    return `header/${count}`;
  }

  return `empty`;
}

function simplifyNewContent(content: NewContentMessage["new"]) {
  if (!content) {
    return undefined;
  }

  return Object.values(content)
    .map((c) => `After: ${c.after} New: ${c.newTransactions.length}`)
    .join(" | ");
}

export function toSimplifiedMessages(
  coValues: Record<string, CoValueCore>,
  messages: {
    from: string;
    to: string;
    msg: SyncMessage;
  }[],
) {
  function getCoValue(id: string) {
    for (const [name, coValue] of Object.entries(coValues)) {
      if (coValue.id === id) {
        return name;
      }
    }

    return `unknown/${id}`;
  }

  function toDebugString(from: string, to: string, msg: SyncMessage) {
    switch (msg.action) {
      case "known":
        return `${from} -> ${to} | KNOWN ${msg.isCorrection ? "CORRECTION " : ""}${getCoValue(msg.id)} sessions: ${simplifySessions(msg)}`;
      case "load":
        return `${from} -> ${to} | LOAD ${getCoValue(msg.id)} sessions: ${simplifySessions(msg)}`;
      case "done":
        return `${from} -> ${to} | DONE ${getCoValue(msg.id)}`;
      case "content":
        return `${from} -> ${to} | CONTENT ${getCoValue(msg.id)} header: ${Boolean(msg.header)} new: ${simplifyNewContent(msg.new)}`;
    }
  }

  return messages.map((m) => toDebugString(m.from, m.to, m.msg));
}

export function nodeRelatedKnownCoValues(node: LocalNode, name: string) {
  const account = node.account as RawControlledAccount;
  const profileID = account.get("profile");
  const profile = profileID && node.expectCoValueLoaded(profileID);
  return {
    [`${name}Account`]: account,
    [`${name}Profile`]: profile,
    [`${name}ProfileGroup`]: profile?.getGroup().core,
  };
}

export function debugMessages(
  coValues: Record<string, CoValueCore>,
  messages: {
    from: string;
    to: string;
    msg: SyncMessage;
  }[],
) {
  console.log(toSimplifiedMessages(coValues, messages));
}
