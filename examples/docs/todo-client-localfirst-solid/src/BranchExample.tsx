import { JazzProvider, createSolidJazzClient } from "jazz-tools/solid";

export function BranchExample(props: { children?: unknown }) {
  const client = createSolidJazzClient(() => ({
    appId: "my-app",
    env: "prod",
    userBranch: "staging",
  }));
  return <JazzProvider client={client}>{props.children as any}</JazzProvider>;
}
