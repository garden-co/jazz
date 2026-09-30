import type { Metadata } from "next";
import "@astryxdesign/core/astryx.css";
import "@garden-co/design/jazz/theme.css";
import "@garden-co/design/jazz/components.css";
import "@garden-co/design/jazz/fonts.css";
import "./globals.css";
import { DesignTheme } from "@/components/design-theme";
import { JazzProvider } from "@/components/jazz-provider";

export const metadata: Metadata = {
  title: "PosterShop",
  description: "Collaborative local-first poster design",
  robots: { index: false, follow: false },
};

export default function Layout({ children }: Readonly<{ children: React.ReactNode }>) {
  return (
    <html lang="en">
      <body>
        <DesignTheme>
          <JazzProvider>{children}</JazzProvider>
        </DesignTheme>
      </body>
    </html>
  );
}
