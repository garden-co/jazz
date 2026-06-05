import { type ParentProps } from "solid-js";
import { JazzProvider, createSolidJazzClient } from "jazz-tools/solid";

export function RuntimeConfigExample(props: ParentProps) {
  const client = createSolidJazzClient(() => ({
    appId: "my-app",
    serverUrl: "https://my-jazz-server.example.com",
    runtimeSources: { baseUrl: "/assets/jazz/" },
  }));
  return <JazzProvider client={client}>{props.children}</JazzProvider>;
}
