import { type ParentProps } from "solid-js";
import { JazzProvider } from "jazz-tools/solid";

export function RuntimeConfigExample(props: ParentProps) {
  return (
    <JazzProvider
      config={{
        appId: "my-app",
        serverUrl: "https://my-jazz-server.example.com",
        runtimeSources: {
          baseUrl: "/assets/jazz/",
          wasmVersion: "2026-08-25", // Change this for every deployed asset build.
        },
      }}
    >
      {props.children}
    </JazzProvider>
  );
}
