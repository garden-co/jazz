import type { Metadata } from "next";
import "@astryxdesign/core/astryx.css";
import "@garden-co/design/jazz/theme.css";
import "@garden-co/design/jazz/components.css";
import "@garden-co/design/jazz/fonts.css";
import "./globals.css";
import { AppTheme } from "@/components/app-theme";
import { JazzProvider } from "@/components/jazz-provider";

export const metadata: Metadata = {
  title: "MusicAgent",
  description: "A booking agent's assistant with durable, streaming replies on Jazz",
  robots: { index: false, follow: false },
};

export default function Layout({ children }: Readonly<{ children: React.ReactNode }>) {
  return (
    <html lang="en">
      <body>
        <AppTheme>
          <JazzProvider>{children}</JazzProvider>
        </AppTheme>
      </body>
    </html>
  );
}
