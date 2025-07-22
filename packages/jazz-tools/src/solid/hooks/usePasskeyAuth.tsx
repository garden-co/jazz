import { BrowserPasskeyAuth } from "jazz-tools/browser";
import { Accessor, createMemo } from "solid-js";
import { useAuthSecretStorage, useJazzContext } from "../context/jazz.js";
import { useIsAuthenticated } from "./useIsAuthenticated.js";

export type PasskeyAuth = {
  readonly auth: Accessor<BrowserPasskeyAuth>;
  readonly state: Accessor<"anonymous" | "signedIn">;
};

type PasskeyAuthParams = {
  readonly appName: string;
  readonly appHostname?: string;
};

/**
 * `usePasskeyAuth` hook provides a `JazzAuth` object for passkey authentication.
 *
 * @example
 * ```ts
 * const auth = usePasskeyAuth(() => ({ appName, appHostname }));
 * ```
 *
 * @category Auth Providers
 */
export const usePasskeyAuth = (params: Accessor<PasskeyAuthParams>) => {
  const jazz = useJazzContext();
  const authSecretStorage = useAuthSecretStorage();
  const isAuthenticated = useIsAuthenticated();

  const auth = createMemo(
    () =>
      new BrowserPasskeyAuth(
        jazz().node.crypto,
        jazz().authenticate,
        authSecretStorage(),
        params().appName,
        params().appHostname,
      ),
  );

  if ("guest" in jazz()) {
    throw new Error("Passkey auth is not supported in guest mode");
  }

  const state = () => (isAuthenticated() ? "signedIn" : "anonymous");

  return { auth, state };
};
