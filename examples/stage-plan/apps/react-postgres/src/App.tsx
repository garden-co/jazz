import type { ReactNode } from "react";
import { QueryClientProvider } from "@tanstack/react-query";
import { Theme } from "@astryxdesign/core";
import { LayerProvider } from "@astryxdesign/core/Layer";
import { jazzTheme } from "@garden-co/design/jazz";
import { SessionProvider, useSignIn } from "./account.js";
import { queryClient } from "./data/live.js";
import { StagePlan } from "./components/StagePlan.js";
import { Loading } from "./components/Loading.js";

export function App() {
  return (
    <Theme theme={jazzTheme} mode="system">
      <LayerProvider>
        <QueryClientProvider client={queryClient}>
          <SessionRoot>
            <StagePlan />
          </SessionRoot>
        </QueryClientProvider>
      </LayerProvider>
    </Theme>
  );
}

function SessionRoot({ children }: { children: ReactNode }) {
  const session = useSignIn();
  if (!session) return <Loading label="Opening StagePlan" />;
  return <SessionProvider value={session}>{children}</SessionProvider>;
}
