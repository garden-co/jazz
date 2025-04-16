import { CoValueCore } from "../exports";
import { CoValueKnownState, NewContentMessage, SyncMessage } from "../sync";

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
    from: "client" | "server";
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

  function toDebugString(from: "client" | "server", msg: SyncMessage) {
    switch (msg.action) {
      case "known":
        return `${from} -> KNOWN ${msg.isCorrection ? "CORRECTION " : ""}${getCoValue(msg.id)} sessions: ${simplifySessions(msg)}`;
      case "load":
        return `${from} -> LOAD ${getCoValue(msg.id)} sessions: ${simplifySessions(msg)}`;
      case "done":
        return `${from} -> DONE ${getCoValue(msg.id)}`;
      case "content":
        return `${from} -> CONTENT ${getCoValue(msg.id)} header: ${Boolean(msg.header)} new: ${simplifyNewContent(msg.new)}`;
    }
  }

  return messages.map((m) => toDebugString(m.from, m.msg));
}

export function debugMessages(
  coValues: Record<string, CoValueCore>,
  messages: {
    from: "client" | "server";
    msg: SyncMessage;
  }[],
) {
  console.log(toSimplifiedMessages(coValues, messages));
}
