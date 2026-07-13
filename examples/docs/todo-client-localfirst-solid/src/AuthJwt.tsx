import { type ParentProps } from "solid-js";
import { JazzProvider, createSolidJazzClient } from "jazz-tools/solid";

export function AuthJwt(props: ParentProps) {
  const client = createSolidJazzClient(() => ({
    appId: "my-app", // Register for an app ID free at v2.dashboard.jazz.tools
    serverUrl: "http://127.0.0.1:4200",
    jwt: "<provider-jwt>",
  }));
  return <JazzProvider client={client}>{props.children}</JazzProvider>;
}
