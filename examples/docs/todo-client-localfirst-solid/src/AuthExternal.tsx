import { createMemo, createSignal, type ParentProps } from "solid-js";
import { JazzProvider, createSolidJazzClient } from "jazz-tools/solid";

export function AuthExternal(props: ParentProps) {
  const appId = "my-app";
  const serverUrl = "http://127.0.0.1:4200";
  const providerJwt = "<provider-jwt>";
  const [hasJwt, setHasJwt] = createSignal(false);

  const localClient = createSolidJazzClient(() => ({ appId, serverUrl }));
  const jwtClient = createSolidJazzClient(() => ({ appId, serverUrl, jwt: providerJwt }));
  const client = createMemo(() => (hasJwt() ? jwtClient : localClient));

  return (
    <JazzProvider client={client()}>
      <button type="button" onClick={() => setHasJwt(true)}>
        Sign in
      </button>
      {props.children}
    </JazzProvider>
  );
}
