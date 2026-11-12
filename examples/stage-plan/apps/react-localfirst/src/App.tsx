import type { ReactNode } from "react";
import { JazzProvider } from "jazz-tools/react";
import { Theme } from "@astryxdesign/core";
import { LayerProvider } from "@astryxdesign/core/Layer";
import { jazzTheme } from "@garden-co/design/jazz";
import { sessionConfig, type SessionOverrides } from "./account.js";
import { StagePlan } from "./components/StagePlan.js";
import { Loading } from "./components/Loading.js";

type AppProps = {
  /** Tests pass their own server, account store and storage; the app otherwise uses its defaults. */
  config?: SessionOverrides;
};

export function App({ config }: AppProps = {}) {
  return (
    <Theme theme={jazzTheme} mode="system">
      <LayerProvider>
        <JazzRoot config={config}>
          <StagePlan />
        </JazzRoot>
      </LayerProvider>
    </Theme>
  );
}

function JazzRoot({ config, children }: AppProps & { children: ReactNode }) {
  return (
    <JazzProvider config={sessionConfig(config)} loading={<Loading label="Opening StagePlan" />}>
      {children}
    </JazzProvider>
  );
}
