import { AgentSecret } from "cojson";
import { Account, ID } from "jazz-tools";

export type MinimalWorkOSClient = {
  user: {
    id: string;
    email: string;
    firstName: string | null;
    lastName: string | null;
    profilePictureUrl: string | null;
    metadata: Record<string, unknown>;
    update: (args: {
        metadata: Record<string, unknown>;
    }) => Promise<unknown>;
  } | null | undefined;
  signOut: () => Promise<void>;
  addListener: (listener: (data: unknown) => void) => void;
};

export type WorkOSCredentials = {
  jazzAccountID: ID<Account>;
  jazzAccountSecret: AgentSecret;
  jazzAccountSeed?: number[];
};

export function isWorkOSCredentials(
  data: NonNullable<MinimalWorkOSClient["user"]>["metadata"] | undefined,
): data is WorkOSCredentials {
  return !!data && "jazzAccountID" in data && "jazzAccountSecret" in data;
}

export function isWorkOSAuthStateEqual(
  previousUser: MinimalWorkOSClient["user"] | null | undefined,
  newUser: MinimalWorkOSClient["user"] | null | undefined,
) {
  if (Boolean(previousUser) !== Boolean(newUser)) {
    return false;
  }

  const previousCredentials = isWorkOSCredentials(previousUser?.metadata);
  const newCredentials = isWorkOSCredentials(newUser?.metadata);

  return previousCredentials === newCredentials;
}