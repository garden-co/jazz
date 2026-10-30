import type { Metadata } from "next";
import "@astryxdesign/core/astryx.css";
import "@garden-co/design/jazz/theme.css";
import "@garden-co/design/jazz/components.css";
import "@garden-co/design/jazz/fonts.css";
import "./globals.css";
import { AppTheme } from "@/components/app-theme";
import { JazzProvider } from "@/components/jazz-provider";

export const metadata: Metadata = {
  title: "Wequencer",
  description: "A collaborative step sequencer built with Jazz, Next.js, and Better Auth",
  robots: { index: false, follow: false },
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
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
