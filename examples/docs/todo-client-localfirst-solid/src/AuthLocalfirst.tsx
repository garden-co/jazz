import { Show, type ParentProps } from "solid-js";
import { JazzProvider, createSolidJazzClient, useLocalFirstAuth } from "jazz-tools/solid";

export function AuthLocalfirst(props: ParentProps) {
  const auth = useLocalFirstAuth();
  return (
    <Show when={!auth.isLoading && auth.secret}>
      {(secret) => {
        const client = createSolidJazzClient(() => ({ appId: "my-app", secret: secret() }));
        return <JazzProvider client={client}>{props.children}</JazzProvider>;
      }}
    </Show>
  );
}
