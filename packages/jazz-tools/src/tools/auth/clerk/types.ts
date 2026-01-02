import { AgentSecret } from "cojson";
import { Account, ID } from "jazz-tools";
import { z } from "zod/v4";

const ClerkUserSchema = z.object({
  fullName: z.string().nullable().optional(),
  username: z.string().nullable().optional(),
  firstName: z.string().nullable().optional(),
  lastName: z.string().nullable().optional(),
  id: z.string().optional(),
  primaryEmailAddress: z
    .object({
      emailAddress: z.string().nullable(),
    })
    .nullable()
    .optional(),
  unsafeMetadata: z.record(z.string(), z.any()),
  update: z.function({
    input: [
      z.object({
        unsafeMetadata: z.record(z.string(), z.any()),
      }),
    ],
    output: z.promise(z.unknown()),
  }),
});

export const ClerkEventSchema = z.object({
  user: ClerkUserSchema.nullable().optional(),
});

export type ClerkUser = z.infer<typeof ClerkUserSchema>;

export type MinimalClerkClient = {
  user: ClerkUser | null | undefined;
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
  data: ClerkUser["unsafeMetadata"] | undefined,
): data is ClerkCredentials {
  return !!data && "jazzAccountID" in data && "jazzAccountSecret" in data;
}

type ClerkUserWithUnsafeMetadata =
  | Pick<ClerkUser, "unsafeMetadata">
  | null
  | undefined;

export function isClerkAuthStateEqual(
  previousUser: ClerkUserWithUnsafeMetadata,
  newUser: ClerkUserWithUnsafeMetadata,
) {
  if (Boolean(previousUser) !== Boolean(newUser)) {
    return false;
  }

  const previousCredentials = isClerkCredentials(previousUser?.unsafeMetadata)
    ? previousUser?.unsafeMetadata
    : null;
  const newCredentials = isClerkCredentials(newUser?.unsafeMetadata)
    ? newUser?.unsafeMetadata
    : null;

  if (!previousCredentials || !newCredentials) {
    return previousCredentials === newCredentials;
  }

  return previousCredentials.jazzAccountID === newCredentials.jazzAccountID;
}
