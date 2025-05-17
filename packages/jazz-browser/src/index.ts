import {
  Account,
  CoValue,
  CoValueClass,
  CoValueOrZodSchema,
  ID,
  InviteSecret,
  createInviteLink as baseCreateInviteLink,
  consumeInviteLink,
} from "jazz-tools";
import { setupInspector } from "./utils/export-account-inspector.js";
export { BrowserPasskeyAuth } from "./auth/PasskeyAuth.js";

setupInspector();

export * from "./createBrowserContext.js";
export * from "./BrowserContextManager.js";

export { LocalStorageKVStore } from "./auth/LocalStorageKVStore.js";

/** @category Invite Links */
export function createInviteLink<C extends CoValue>(
  value: C,
  role: "reader" | "writer" | "admin" | "writeOnly",
  // default to same address as window.location, but without hash
  {
    baseURL = window.location.href.replace(/#.*$/, ""),
    valueHint,
  }: { baseURL?: string; valueHint?: string } = {},
): string {
  return baseCreateInviteLink(value, role, baseURL, valueHint);
}

/** @category Invite Links */
export { parseInviteLink } from "jazz-tools";

/** @category Invite Links */
export async function consumeInviteLinkFromWindowLocation<
  S extends CoValueOrZodSchema,
>({
  as,
  forValueHint,
  invitedObjectSchema,
}: {
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
  const result = await consumeInviteLink({
    inviteURL: window.location.href,
    as,
    forValueHint,
    invitedObjectSchema,
  });

  if (result) {
    window.history.replaceState(
      {},
      "",
      window.location.href.replace(/#.*$/, ""),
    );
  }

  return result;
}
