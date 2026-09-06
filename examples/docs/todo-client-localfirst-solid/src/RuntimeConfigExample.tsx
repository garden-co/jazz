import { type ParentProps } from "solid-js";
import type { AccountHandle } from "jazz-tools";
import { JazzProvider } from "jazz-tools/solid";

export function RuntimeConfigExample(props: ParentProps<{ account: AccountHandle }>) {
  return (
    <JazzProvider
      config={{
        appId: "my-app",
        account: props.account, // Prepare with these same runtimeSources.
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
