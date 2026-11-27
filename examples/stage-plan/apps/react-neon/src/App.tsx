import type { ReactNode } from "react";
import { Theme } from "@astryxdesign/core";
import { LayerProvider } from "@astryxdesign/core/Layer";
import { jazzTheme } from "@garden-co/design/jazz";
import { DbProvider, useAccount } from "./account.js";
import { StagePlan } from "./components/StagePlan.js";
import { Loading } from "./components/Loading.js";

export function App() {
  return (
    <Theme theme={jazzTheme} mode="system">
      <LayerProvider>
        <AccountRoot>
          <StagePlan />
        </AccountRoot>
      </LayerProvider>
    </Theme>
  );
}

function AccountRoot({ children }: { children: ReactNode }) {
  const userID = useAccount();
  if (!userID) return <Loading label="Opening StagePlan" />;
  return <DbProvider value={{ userID }}>{children}</DbProvider>;
}
