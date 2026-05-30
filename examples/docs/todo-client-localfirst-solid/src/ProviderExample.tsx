import { JazzProvider, createSolidJazzClient } from "jazz-tools/solid";

export function ProviderExample(props: { children?: unknown }) {
  const client = createSolidJazzClient(() => ({ appId: "my-app" }));
  return (
    <JazzProvider client={client} fallback={<p>Loading...</p>}>
      {props.children as any}
    </JazzProvider>
  );
}
