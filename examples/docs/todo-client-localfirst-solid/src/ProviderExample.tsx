import { type ParentProps } from "solid-js";
import { JazzProvider } from "jazz-tools/solid";

export function ProviderExample(props: ParentProps) {
  return (
    <JazzProvider config={{ appId: "my-app" }} fallback={<p>Loading...</p>}>
      {props.children}
    </JazzProvider>
  );
}
