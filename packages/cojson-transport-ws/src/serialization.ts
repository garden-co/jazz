import { type SyncMessage, logger } from "cojson";
import type { PingMsg } from "./types.js";

export function getErrorMessage(error: unknown) {
  return error instanceof Error ? error.message : "Unknown error";
}

export function addMessageToBacklog(backlog: string, message: SyncMessage) {
  if (!backlog) {
    return JSON.stringify(message);
  }
  return `${backlog}\n${JSON.stringify(message)}`;
}

export function deserializeMessages(messages: unknown) {
  if (typeof messages !== "string") {
    return {
      ok: false,
      error: new Error("Expected a string"),
    } as const;
  }

  try {
    return {
      ok: true,
      messages: messages.split("\n").map((msg) => JSON.parse(msg)) as
        | SyncMessage[]
        | PingMsg[],
    } as const;
  } catch (e) {
    logger.error("Error while deserializing messages", { err: e });
    return {
      ok: false,
      error: e,
    } as const;
  }
}
