import type { Metadata } from "next";
import "@astryxdesign/core/astryx.css";
import "@garden-co/design/jazz/theme.css";
import "@garden-co/design/jazz/components.css";
import "@garden-co/design/jazz/fonts.css";
import "./globals.css";
import { AppTheme } from "@/components/app-theme";
import { InstantProvider } from "@/components/instant-provider";

export const metadata: Metadata = {
  title: "Wequencer",
  description: "A collaborative step sequencer built with InstantDB and Next.js",
  robots: { index: false, follow: false },
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
      <body>
        <AppTheme>
          <InstantProvider>{children}</InstantProvider>
        </AppTheme>
      </body>
    </html>
  );
}
