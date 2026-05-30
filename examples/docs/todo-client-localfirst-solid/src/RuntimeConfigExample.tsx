import { JazzProvider, createSolidJazzClient } from "jazz-tools/solid";

export function RuntimeConfigExample(props: { children?: unknown }) {
  const client = createSolidJazzClient(() => ({
    appId: "my-app",
    serverUrl: "https://my-jazz-server.example.com",
    runtimeSources: { baseUrl: "/assets/jazz/" },
  }));
  return <JazzProvider client={client}>{props.children as any}</JazzProvider>;
}
