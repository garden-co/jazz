/**
 * `<JazzProvider>` used to take the client configuration as its own props
 * (`<JazzProvider serverUrl=…>`), or a Db config with an `account`, and
 * `fallback`. Name the change instead of failing far from it.
 */
export function refuseRemovedProviderProps(props: object): void {
  const given = props as Record<string, unknown>;
  if ("fallback" in given)
    throw new Error(
      "<JazzProvider> has no `fallback`: it shows `loading` while starting, `signedOut` while no one is signed in, and `error` when it can't start.",
    );
  if (given.config && typeof given.config === "object" && "account" in given.config)
    throw new Error(
      "<JazzProvider config> is the session's configuration and takes no `account`: the session signs in itself (createDeviceAccount(), or your backend's user). For a client you opened yourself, use <JazzClientProvider client>.",
    );
  if (!("config" in given) && !("session" in given))
    throw new Error(
      "<JazzProvider> takes its configuration as `config={{ serverUrl, … }}` (or a `session` from createJazzClient), not as its own props.",
    );
}
