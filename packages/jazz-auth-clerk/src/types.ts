import { AgentSecret } from "cojson";
import { Account, ID } from "jazz-tools";

export type MinimalClerkClient = {
  user:
    | {
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        unsafeMetadata: Record<string, any>;
        fullName: string | null;
        username: string | null;
        firstName: string | null;
        lastName: string | null;
        id: string;
        primaryEmailAddress: {
          emailAddress: string | null;
        } | null;
        update: (args: {
          // eslint-disable-next-line @typescript-eslint/no-explicit-any
          unsafeMetadata: Record<string, any>;
        }) => Promise<unknown>;
      }
    | null
    | undefined;
  signOut: () => Promise<void>;
  addListener: (listener: (data: unknown) => void) => void;
};

export type ClerkCredentials = {
  jazzAccountID: ID<Account>;
  jazzAccountSecret: AgentSecret;
  jazzAccountSeed?: number[];
};

/**
 * Checks if the Clerk user metadata contains the necessary credentials for Jazz auth.
 * **Note**: It does not validate the credentials, only checks if the necessary fields are present in the metadata object.
 */
export function isClerkCredentials(
  data: NonNullable<MinimalClerkClient["user"]>["unsafeMetadata"] | undefined,
): data is ClerkCredentials {
  return !!data && "jazzAccountID" in data && "jazzAccountSecret" in data;
}

export function isClerkAuthStateEqual(
  previousUser: MinimalClerkClient["user"] | null | undefined,
  newUser: MinimalClerkClient["user"] | null | undefined,
) {
  if (Boolean(previousUser) !== Boolean(newUser)) {
    return false;
  }

  const previousCredentials = isClerkCredentials(previousUser?.unsafeMetadata);
  const newCredentials = isClerkCredentials(newUser?.unsafeMetadata);

  return previousCredentials === newCredentials;
}
