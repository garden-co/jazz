import { type ParentProps } from "solid-js";
import { JazzProvider, createSolidJazzClient } from "jazz-tools/solid";

export function BranchExample(props: ParentProps) {
  const client = createSolidJazzClient(() => ({
    appId: "my-app",
    env: "prod",
    userBranch: "staging",
  }));
  return <JazzProvider client={client}>{props.children}</JazzProvider>;
}
