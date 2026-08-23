import { Show, type ParentProps } from "solid-js";
import { JazzProvider, useLocalFirstAuth } from "jazz-tools/solid";

export function AuthLocalfirst(props: ParentProps) {
  const auth = useLocalFirstAuth();
  return (
    <Show when={!auth.isLoading && auth.secret}>
      {(secret) => {
        return (
          <JazzProvider config={{ appId: "my-app", secret: secret() }}>
            {props.children}
          </JazzProvider>
        );
      }}
    </Show>
  );
}
