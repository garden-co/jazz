import { type InviteSecret, cojsonInternals } from "cojson";
import { Account } from "../coValues/account.js";
import type {
  CoValue,
  CoValueClass,
  CoValueClassOrSchema,
  ID,
} from "../internal.js";

/** @category Invite Links */
export function createInviteLink<C extends CoValue>(
  value: C,
  role: "reader" | "writer" | "admin" | "writeOnly",
  baseURL: string,
  valueHint?: string,
): string {
  const coValueCore = value.$jazz.raw.core;
  let currentCoValue = coValueCore;

  while (currentCoValue.verified.header.ruleset.type === "ownedByGroup") {
    currentCoValue = currentCoValue.getGroup().core;
  }

  const { ruleset, meta } = currentCoValue.verified.header;

  if (ruleset.type !== "group" || meta?.type === "account") {
    throw new Error("Can't create invite link for object without group");
  }

  const group = cojsonInternals.expectGroup(currentCoValue.getCurrentContent());
  const inviteSecret = group.createInvite(role);

  return `${baseURL}#/invite/${valueHint ? valueHint + "/" : ""}${
    value.id
  }/${inviteSecret}`;
}

/** @category Invite Links */
export function parseInviteLink(inviteURL: string):
  | {
      valueID: string;
      valueHint?: string;
      inviteSecret: InviteSecret;
    }
  | undefined {
  const url = new URL(inviteURL);
  const parts = url.hash.split("/");

  let valueHint: string | undefined;
  let valueID: string | undefined;
  let inviteSecret: InviteSecret | undefined;

  if (parts[0] === "#" && parts[1] === "invite") {
    if (parts.length === 5) {
      valueHint = parts[2];
      valueID = parts[3];
      inviteSecret = parts[4] as InviteSecret;
    } else if (parts.length === 4) {
      valueID = parts[2];
      inviteSecret = parts[3] as InviteSecret;
    }

    if (!valueID || !inviteSecret) {
      return undefined;
    }
    return { valueID, inviteSecret, valueHint };
  }
}

/** @category Invite Links */
export function consumeInviteLink<S extends CoValueClassOrSchema>({
  inviteURL,
  as = Account.getMe(),
  forValueHint,
  invitedObjectSchema,
}: {
  inviteURL: string;
  as?: Account;
  forValueHint?: string;
  invitedObjectSchema: S;
}): Promise<
  | {
      valueID: string;
      valueHint?: string;
      inviteSecret: InviteSecret;
    }
  | undefined
> {
  return new Promise((resolve, reject) => {
    const result = parseInviteLink(inviteURL);

    if (result && result.valueHint === forValueHint) {
      as.$jazz
        .acceptInvite(result.valueID, result.inviteSecret, invitedObjectSchema)
        .then(() => {
          resolve(result);
        })
        .catch(reject);
    } else {
      resolve(undefined);
    }
  });
}
