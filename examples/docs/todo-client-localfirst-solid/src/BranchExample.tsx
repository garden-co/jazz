import { type ParentProps } from "solid-js";
import { JazzProvider } from "jazz-tools/solid";

export function BranchExample(props: ParentProps) {
  return (
    <JazzProvider config={{ appId: "my-app", env: "prod", userBranch: "staging" }}>
      {props.children}
    </JazzProvider>
  );
}
