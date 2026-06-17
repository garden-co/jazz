import { type ParentProps } from "solid-js";
import { JazzProvider, createSolidJazzClient } from "jazz-tools/solid";

export function AuthOffline(props: ParentProps) {
  const client = createSolidJazzClient(() => ({ appId: "my-app" }));
  return <JazzProvider client={client}>{props.children}</JazzProvider>;
}
