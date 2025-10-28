import { type CoValueHeader } from "./coValueCore/verifiedState.js";

/**
 * The priority of a `CoValue` determines how much priority is given
 * to its content messages.
 *
 * The priority value is handled as weight in the weighed round robin algorithm
 * used to determine the order in which messages are sent.
 *
 * Loosely follows the HTTP urgency range and order, but limited to 3 values:
 *  - https://www.rfc-editor.org/rfc/rfc9218.html#name-urgency
 */
export const CO_VALUE_PRIORITY = {
  HIGH: 0,
  MEDIUM: 3,
  LOW: 6,
} as const;

export type CoValuePriority = 0 | 3 | 6;

export function getPriorityFromHeader(
  header: CoValueHeader | undefined | boolean,
): CoValuePriority {
  if (typeof header === "boolean" || !header) {
    return CO_VALUE_PRIORITY.MEDIUM;
  }

  if (header.meta?.type === "account") {
    return CO_VALUE_PRIORITY.HIGH;
  }

  if (header.ruleset.type === "group") {
    return CO_VALUE_PRIORITY.HIGH;
  }

  if (header.type === "costream" && header.meta?.type === "binary") {
    return CO_VALUE_PRIORITY.LOW;
  }

  return CO_VALUE_PRIORITY.MEDIUM;
}
