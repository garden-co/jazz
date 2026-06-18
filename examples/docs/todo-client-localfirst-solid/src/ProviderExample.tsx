import { type ParentProps } from "solid-js";
import { JazzProvider, createSolidJazzClient } from "jazz-tools/solid";

export function ProviderExample(props: ParentProps) {
  const client = createSolidJazzClient(() => ({ appId: "my-app" }));
  return (
    <JazzProvider client={client} fallback={<p>Loading...</p>}>
      {props.children}
    </JazzProvider>
  );
}
