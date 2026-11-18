import { AccountAuthError, type Session } from "jazz-tools";
import { type LocalFirstGuest } from "jazz-tools/backend";

// Storage mode, not auth: these stay.
export const tier = { tier: "local-first" } as const;

export function isDeviceAccount(session: Session): boolean {
  return session.authMode === "local-first";
}

export function describe(session: Session): string {
  switch (session.authMode) {
    case "local-first":
      return "this device";
    default:
      return "signed in";
  }
}

export function needsAccount(error: AccountAuthError): boolean {
  return error.code === "local_first_account_required";
}

export async function proof(db: { getLocalFirstIdentityProof(o: object): Promise<string> }) {
  return db.getLocalFirstIdentityProof({ audience: "api" });
}

export function answer(guest: LocalFirstGuest): LocalFirstGuest {
  return guest;
}
