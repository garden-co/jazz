import * as React from "react";
import { JazzProvider } from "jazz-tools/react";
import type { DbConfig } from "jazz-tools";
import { Center } from "@astryxdesign/core/Center";
import { Spinner } from "@astryxdesign/core/Spinner";
import { prepareAccountConfig } from "./account.js";
import { FileBrowser } from "./FileBrowser.js";

function Opening() {
  return (
    <Center height="100dvh">
      <Spinner label="Opening EpicDrop" />
    </Center>
  );
}

export function App() {
  const [config, setConfig] = React.useState<DbConfig>();
  const [error, setError] = React.useState<Error>();
  React.useEffect(() => {
    let cancelled = false;
    prepareAccountConfig().then(
      (value) => {
        if (!cancelled) setConfig(value);
      },
      (cause) => {
        if (!cancelled) setError(cause instanceof Error ? cause : new Error(String(cause)));
      },
    );
    return () => {
      cancelled = true;
    };
  }, []);
  if (error) throw error;
  if (!config) return <Opening />;
  return (
    <JazzProvider config={config} fallback={<Opening />}>
      <FileBrowser />
    </JazzProvider>
  );
}
