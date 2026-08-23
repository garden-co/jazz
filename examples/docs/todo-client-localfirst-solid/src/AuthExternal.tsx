import { createMemo, createSignal, type ParentProps } from "solid-js";
import { JazzProvider } from "jazz-tools/solid";

export function AuthExternal(props: ParentProps) {
  const appId = "my-app";
  const serverUrl = "http://127.0.0.1:4200";
  const providerJwt = "<provider-jwt>";
  const [hasJwt, setHasJwt] = createSignal(false);

  const config = createMemo(() =>
    hasJwt() ? { appId, serverUrl, jwtToken: providerJwt } : { appId, serverUrl },
  );

  return (
    <JazzProvider config={config()}>
      <button type="button" onClick={() => setHasJwt(true)}>
        Sign in
      </button>
      {props.children}
    </JazzProvider>
  );
}
