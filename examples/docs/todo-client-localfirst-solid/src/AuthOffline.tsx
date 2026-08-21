import { type ParentProps } from "solid-js";
import { JazzProvider } from "jazz-tools/solid";

export function AuthOffline(props: ParentProps) {
  return <JazzProvider config={{ appId: "my-app" }}>{props.children}</JazzProvider>;
}
