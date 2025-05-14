import type { ClientOptions } from "better-auth";
import type {
  anonymousClient,
  apiKeyClient,
  emailOTPClient,
  genericOAuthClient,
  magicLinkClient,
  multiSessionClient,
  oidcClient,
  oneTapClient,
  passkeyClient,
  phoneNumberClient,
  ssoClient,
  twoFactorClient,
  usernameClient,
} from "better-auth/client/plugins";
import type {
  AuthClient as GenericAuthClient,
  Session as GenericSession,
  User as GenericUser,
} from "jazz-auth-betterauth";
import type { useAuth } from "../contexts/Auth.js";

export type AuthClient = ReturnType<typeof useAuth>["authClient"];
export type Options = AuthClient extends GenericAuthClient<
  infer R extends ClientOptions
>
  ? R
  : never;
declare const listAccounts: AuthClient["listAccounts"];
export type AccountsType = Awaited<ReturnType<typeof listAccounts<{}>>>;
export type SSOProviderType = Parameters<
  AuthClient["signIn"]["social"]
>[0]["provider"];
export type Session = GenericSession<Options>;
export type User = GenericUser<Options>;

export type FullAuthClient = GenericAuthClient<{
  plugins: [
    ReturnType<typeof twoFactorClient>,
    ReturnType<typeof usernameClient>,
    ReturnType<typeof anonymousClient>,
    ReturnType<typeof phoneNumberClient>,
    ReturnType<typeof magicLinkClient>,
    ReturnType<typeof emailOTPClient>,
    ReturnType<typeof passkeyClient>,
    ReturnType<typeof genericOAuthClient>,
    ReturnType<typeof oneTapClient>,
    ReturnType<typeof apiKeyClient>,
    ReturnType<typeof oidcClient>,
    ReturnType<typeof ssoClient>,
    ReturnType<typeof multiSessionClient>,
  ];
}>;
