import { ConvexAuthProvider } from "@convex-dev/auth/react";
import { Theme } from "@astryxdesign/core";
import { LayerProvider } from "@astryxdesign/core/Layer";
import { jazzTheme } from "@garden-co/design/jazz";
import { convex } from "./account.js";
import { StagePlan } from "./components/StagePlan.js";

export function App() {
  return (
    <Theme theme={jazzTheme} mode="system">
      <LayerProvider>
        <ConvexAuthProvider client={convex}>
          <StagePlan />
        </ConvexAuthProvider>
      </LayerProvider>
    </Theme>
  );
}
