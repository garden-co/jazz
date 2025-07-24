import { BrowserPasskeyAuth } from "jazz-tools/browser";
import { Accessor, createMemo } from "solid-js";
import { useAuthSecretStorage, useJazzContext } from "../jazz.js";
import { useIsAuthenticated } from "./useIsAuthenticated.js";

export type PasskeyAuth = {
  readonly auth: Accessor<BrowserPasskeyAuth>;
  readonly state: Accessor<"anonymous" | "signedIn">;
};

/** @category Auth Providers */
export function usePasskeyAuth({
  appName,
  appHostname,
}: {
  appName: string;
  appHostname?: string;
}): PasskeyAuth {
  const jazz = useJazzContext();
  const authSecretStorage = useAuthSecretStorage();
  const isAuthenticated = useIsAuthenticated();

  const auth = createMemo(
    () =>
      new BrowserPasskeyAuth(
        jazz().node.crypto,
        jazz().authenticate,
        authSecretStorage(),
        appName,
        appHostname,
      ),
  );

  if ("guest" in jazz()) {
    throw new Error("Passkey auth is not supported in guest mode");
  }

  const state = () => (isAuthenticated() ? "signedIn" : "anonymous");

  return { auth, state };
}
