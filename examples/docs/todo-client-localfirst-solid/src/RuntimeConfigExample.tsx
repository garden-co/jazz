import { type ParentProps } from "solid-js";
import { JazzProvider } from "jazz-tools/solid";

export function RuntimeConfigExample(props: ParentProps) {
  return (
    <JazzProvider
      config={{
        appId: "my-app",
        serverUrl: "https://my-jazz-server.example.com",
        runtimeSources: { baseUrl: "/assets/jazz/" },
      }}
    >
      {props.children}
    </JazzProvider>
  );
}
