import { type ParentProps } from "solid-js";
import { JazzProvider } from "jazz-tools/solid";

// No server: Jazz stays on this device, with one device account per browser profile.
export function AuthOffline(props: ParentProps) {
  return (
    <JazzProvider
      config={{ serverUrl: null, client: () => import("jazz-tools/local-first") }}
      loading={<p>Loading...</p>}
    >
      {props.children}
    </JazzProvider>
  );
}
