import type { ReactNode } from "react";
import type { DbConfig } from "jazz-tools";
import { JazzProvider, JazzSessionProvider } from "jazz-tools/react";
import { Theme } from "@astryxdesign/core";
import { LayerProvider } from "@astryxdesign/core/Layer";
import { jazzTheme } from "@garden-co/design/jazz";
import { sessionConfig } from "./account.js";
import { StagePlan } from "./components/StagePlan.js";
import { Loading } from "./components/Loading.js";

type AppProps = {
  /** Tests pass a prepared account and storage; the app otherwise uses a local-first session. */
  config?: Partial<DbConfig>;
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
  const fallback = <Loading label="Opening StagePlan" />;
  if (config?.account) {
    return (
      <JazzProvider
        config={{ appId: config.appId!, env: "dev", ...config, account: config.account }}
        fallback={fallback}
      >
        {children}
      </JazzProvider>
    );
  }
  return (
    <JazzSessionProvider config={sessionConfig(config)} fallback={fallback}>
      {children}
    </JazzSessionProvider>
  );
}
