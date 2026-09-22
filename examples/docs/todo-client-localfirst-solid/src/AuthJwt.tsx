import { type ParentProps } from "solid-js";
import type { DbConfig } from "jazz-tools";
import { JazzProvider } from "jazz-tools/solid";

// Prepare a handle outside this context with createAccountManager.
export function AuthJwt(props: ParentProps<{ config: DbConfig }>) {
  return (
    <JazzProvider config={props.config} fallback={<p>Loading...</p>}>
      {props.children}
    </JazzProvider>
  );
}
