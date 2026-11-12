import { JazzProvider } from "jazz-tools/react";
import { Center } from "@astryxdesign/core/Center";
import { Spinner } from "@astryxdesign/core/Spinner";
import { FileBrowser } from "./FileBrowser.js";

function Opening() {
  return (
    <Center height="100dvh">
      <Spinner label="Opening EpicDrop" />
    </Center>
  );
}

export function App() {
  return (
    <JazzProvider
      config={{
        // The app's backend serves Jazz on this origin (`/jazz`, the default).
        env: "dev",
        // Every visitor starts as a device account: no login.
        initial: "device",
        // The local-first client: files stay readable offline.
        client: () => import("jazz-tools/local-first"),
      }}
      loading={<Opening />}
    >
      <FileBrowser />
    </JazzProvider>
  );
}
