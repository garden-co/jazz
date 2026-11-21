import type { ReactNode } from "react";
import { ZeroProvider } from "@rocicorp/zero/react";
import { Theme } from "@astryxdesign/core";
import { LayerProvider } from "@astryxdesign/core/Layer";
import { jazzTheme } from "@garden-co/design/jazz";
import { schema } from "../schema.js";
import { mutators } from "../mutators.js";
import { cacheURL, useAccount } from "./account.js";
import { StagePlan } from "./components/StagePlan.js";
import { Loading } from "./components/Loading.js";

export function App() {
  return (
    <Theme theme={jazzTheme} mode="system">
      <LayerProvider>
        <ZeroRoot>
          <StagePlan />
        </ZeroRoot>
      </LayerProvider>
    </Theme>
  );
}

function ZeroRoot({ children }: { children: ReactNode }) {
  const userID = useAccount();
  if (!userID) return <Loading label="Opening StagePlan" />;
  return (
    <ZeroProvider userID={userID} context={{ userID }} cacheURL={cacheURL} schema={schema} mutators={mutators}>
      {children}
    </ZeroProvider>
  );
}
