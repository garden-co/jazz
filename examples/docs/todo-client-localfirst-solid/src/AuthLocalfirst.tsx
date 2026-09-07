import { type ParentProps } from "solid-js";
import type { JazzSession } from "jazz-tools/solid";
import type { JazzClient } from "jazz-tools/client";
import { JazzSessionProvider } from "jazz-tools/solid";

// Configure once with await createJazzSession({ appId, serverUrl, initial: "local-first" }).
export function AuthLocalfirst(props: ParentProps<{ session: JazzSession<JazzClient> }>) {
  return (
    <JazzSessionProvider session={props.session} fallback={<p>Loading...</p>}>
      {props.children}
    </JazzSessionProvider>
  );
}
