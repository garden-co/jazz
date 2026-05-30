import { Show } from "solid-js";
import { JazzProvider, createSolidJazzClient, useLocalFirstAuth } from "jazz-tools/solid";

export function AuthLocalfirst(props: { children?: unknown }) {
  const auth = useLocalFirstAuth();
  return (
    <Show when={!auth.isLoading && auth.secret}>
      {(secret) => {
        const client = createSolidJazzClient(() => ({ appId: "my-app", secret: secret() }));
        return <JazzProvider client={client}>{props.children as any}</JazzProvider>;
      }}
    </Show>
  );
}
