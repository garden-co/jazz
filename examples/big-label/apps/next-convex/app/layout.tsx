import type { Metadata } from "next";
import type { ReactNode } from "react";
import "@astryxdesign/core/reset.css";
import "@astryxdesign/core/astryx.css";
import "@garden-co/design/jazz/theme.css";
import "@garden-co/design/jazz/components.css";
import "@garden-co/design/jazz/fonts.css";
import "../src/app.css";
import { ConvexAuthNextjsServerProvider } from "@convex-dev/auth/nextjs/server";
import { JazzTheme } from "../src/theme";

export const metadata: Metadata = {
  title: "BigLabel",
  description: "Multi-tenant record-label operations on Convex.",
  robots: { index: false, follow: false },
};

export default function Layout({ children }: { children: ReactNode }) {
  return (
    <ConvexAuthNextjsServerProvider>
      <html lang="en">
        <body>
          <JazzTheme>{children}</JazzTheme>
        </body>
      </html>
    </ConvexAuthNextjsServerProvider>
  );
}
