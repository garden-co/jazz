import { type AgentSecret } from "cojson";
import { z } from "zod/v4";

const ClerkJazzCredentialsSchema = z.object({
  jazzAccountID: z.string(),
  jazzAccountSecret: z.string(),
  jazzAccountSeed: z.array(z.number()).optional(),
});

const ClerkUserSchema = z.object({
  fullName: z.string().nullish(),
  username: z.string().nullish(),
  firstName: z.string().nullish(),
  lastName: z.string().nullish(),
  id: z.string().optional(),
  primaryEmailAddress: z
    .object({
      emailAddress: z.string().nullable(),
    })
    .nullish(),
  unsafeMetadata: z.union([z.object({}), ClerkJazzCredentialsSchema]),
  update: z.function({
    input: [
      z.object({
        unsafeMetadata: ClerkJazzCredentialsSchema,
      }),
    ],
    output: z.promise(z.unknown()),
  }),
});

export const ClerkEventSchema = z.object({
  user: ClerkUserSchema.nullish(),
});

export type ClerkUser = z.infer<typeof ClerkUserSchema>;

export type MinimalClerkClient = {
  user: ClerkUser | null | undefined;
  signOut: () => Promise<void>;
  addListener: (listener: (data: unknown) => void) => void;
};

export type ClerkCredentials = {
  jazzAccountID: string;
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
