import { type ParentProps } from "solid-js";
import { JazzProvider, type JazzClient, type JazzSession } from "jazz-tools/solid";

// Open once with await createJazzClient({ serverUrl, initial: "device", client }).
export function AuthLocalfirst(props: ParentProps<{ session: JazzSession<JazzClient> }>) {
  return (
    <JazzProvider session={props.session} loading={<p>Loading...</p>}>
      {props.children}
    </JazzProvider>
  );
}
