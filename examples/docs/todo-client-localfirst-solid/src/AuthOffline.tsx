import { JazzProvider, createSolidJazzClient } from "jazz-tools/solid";

export function AuthOffline(props: { children?: unknown }) {
  const client = createSolidJazzClient(() => ({ appId: "my-app" }));
  return <JazzProvider client={client}>{props.children as any}</JazzProvider>;
}
