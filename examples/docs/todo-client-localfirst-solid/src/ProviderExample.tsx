import { type ParentProps } from "solid-js";
import { JazzProvider, type JazzClientConfig } from "jazz-tools/solid";

export function ProviderExample(props: ParentProps<{ config: JazzClientConfig }>) {
  return (
    <JazzProvider config={props.config} loading={<p>Loading...</p>}>
      {props.children}
    </JazzProvider>
  );
}
