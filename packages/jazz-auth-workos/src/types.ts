import { AgentSecret } from "cojson";
import { Account, ID } from "jazz-tools";

// A copy of the WorkOS redirect options needed for the signIn abstraction
// Since RedirectOptions is not exported directly from @workos-inc/authkit-js
export interface RedirectOptions {
  /**
   *  @deprecated We previously required initiate login endpoints to return the `context`
   *  query parameter when getting the authorization URL. This is no longer necessary.
   */
  context?: string;
  invitationToken?: string;
  loginHint?: string;
  organizationId?: string;
  passwordResetToken?: string;
  state?: any;
  type: "sign-in" | "sign-up";
}

export type WorkOSAuthHook = {
  getAccessToken: (options?: {
    forceRefresh?: boolean;
  }) => Promise<string>
  switchToOrganization: ({ organizationId, signInOpts, }: {
    organizationId: string;
    signInOpts?: Omit<RedirectOptions, "type" | "organizationId">;
  }) => Promise<void>
  signIn: (opts?: Omit<RedirectOptions, "type">) => Promise<void>;
  signUp: (opts?: Omit<RedirectOptions, "type">) => Promise<void>;
  signOut: (options?: { returnTo?: string, navigate?: true }) => void;
  isLoading: boolean;
  role: string | null;
  organizationId: string | null;
  permissions: string[];
  user: {
    id: string;
    email: string;
    firstName: string | null;
    lastName: string | null;
    profilePictureUrl: string | null;
  } | null | undefined;
};

export type JazzCredentials = {
  jazzAccountID: ID<Account>;
  jazzAccountSecret: AgentSecret;
  jazzAccountSeed?: number[];
};

export function isJazzCredentials(
  data: JazzCredentials | null | undefined,
): data is JazzCredentials {
  return !!data && "jazzAccountID" in data && "jazzAccountSecret" in data;
}

export function isWorkOSAuthStateEqual(
  prevCredentails: JazzCredentials | null | undefined,
  newCredentials: JazzCredentials | null | undefined,
) {
  if (Boolean(prevCredentails) !== Boolean(newCredentials)) {
    return false;
  }

  return isJazzCredentials(prevCredentails) && isJazzCredentials(newCredentials) && prevCredentails === newCredentials;
}