import { type ParentProps } from "solid-js";
import type { DbConfig } from "jazz-tools";
import { JazzProvider } from "jazz-tools/solid";

// Prepare a handle outside this context with createAccountManager.
export function AuthOffline(props: ParentProps<{ config: DbConfig }>) {
  return (
    <JazzProvider config={{ ...props.config, serverUrl: undefined }} fallback={<p>Loading...</p>}>
      {props.children}
    </JazzProvider>
  );
}
